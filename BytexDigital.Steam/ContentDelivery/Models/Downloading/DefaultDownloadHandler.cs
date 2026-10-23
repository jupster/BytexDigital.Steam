using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using BytexDigital.Steam.ContentDelivery.Enumerations;
using BytexDigital.Steam.ContentDelivery.Exceptions;
using BytexDigital.Steam.Core.Structs;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx;
using SteamKit2.CDN;
using static SteamKit2.DepotManifest;

namespace BytexDigital.Steam.ContentDelivery.Models.Downloading
{
    public class DefaultDownloadHandler : IDownloadHandler, IAsyncDisposable, IDisposable
    {
        //private CancellationTokenSource _cancellationTokenSource;
        internal byte[] _depotKey;
        internal AsyncAutoResetEvent _eventHandlerCompleted = new AsyncAutoResetEvent(true);
        internal int _eventHandlersRunning;

        private readonly ConcurrentDictionary<ManifestFile, FileTarget> _fileTargets = new();

        private readonly ConcurrentBag<FileWriter> _fileWriters = new();
        private SteamCdnServerPool _serverPool;
        private ConcurrentBag<ChunkJob> _chunks = new ConcurrentBag<ChunkJob>();
        internal SteamContentClient _steamContentClient;

        public Manifest Manifest { get; }
        public AppId AppId { get; }
        public DepotId DepotId { get; }
        public ManifestId ManifestId { get; }
        public DownloadHandlerStateEnum State { get; protected set; }
        public string DownloadDirectory { get; protected set; }
        public Func<ManifestFile, bool> FileCondition { get; protected set; }

        public ILogger Logger { get; set; }

        public DefaultDownloadHandler(
            SteamContentClient steamContentClient,
            Manifest manifest,
            AppId appId,
            DepotId depotId)
        {
            _steamContentClient = steamContentClient;

            Manifest = manifest;
            AppId = appId;
            DepotId = depotId;
            ManifestId = manifest.ManifestId;
        }

        public event EventHandler<ManifestFile> FileDownloaded;
        public event EventHandler<FileVerifiedArgs> FileVerified;
        public event EventHandler<VerificationCompletedArgs> VerificationCompleted;
        public event EventHandler<EventArgs> DownloadComplete;

        public Task SetupAsync(
            string directory,
            Func<ManifestFile, bool> condition,
            CancellationToken cancellationToken = default)
        {
            if (State != DownloadHandlerStateEnum.Created)
            {
                throw new DownloadHandlerStateMismatchException(DownloadHandlerStateEnum.Created, State);
            }

            DownloadDirectory = directory;
            FileCondition = condition;
            
            State = DownloadHandlerStateEnum.SetUp;

            return Task.CompletedTask;
        }

        public int TotalFileCount
        {
            get
            {
                if (State >= DownloadHandlerStateEnum.Verified)
                {
                    return _fileTargets.Count;
                }
                else
                {
                    return Manifest.Files.Count;
                }
            }
        }

        public ulong TotalFileSize
        {
            get
            {
                if (State >= DownloadHandlerStateEnum.Verified)
                {
                    return _fileTargets.Select(x => x.Key.TotalSize).Aggregate(0UL, (a, b) => a + b);
                }
                else
                {
                    return Manifest.Files.Select(x => x.TotalSize).Aggregate(0UL, (a, b) => a + b);
                }
            }
        }

        public double TotalProgress
        {
            get
            {
                if (State >= DownloadHandlerStateEnum.Verified)
                {
                    var totalBytes = _fileTargets.Sum(x => (long) x.Value.TotalBytes);
                    var currentBytes = _fileTargets.Sum(x => (long) x.Value.WrittenBytes);

                    return totalBytes > 0 ? (double) currentBytes / totalBytes : 1;
                }
                else
                {
                    return 0;
                }
            }
        }

        public async Task VerifyAsync(CancellationToken cancellationToken = default)
        {
            if (State != DownloadHandlerStateEnum.SetUp)
            {
                throw new DownloadHandlerStateMismatchException(DownloadHandlerStateEnum.SetUp, State);
            }

            try
            {
                State = DownloadHandlerStateEnum.Verifying;

                // Filter out files that are directories or that the caller does not want
                IEnumerable<ManifestFile> filteredFiles = Manifest.Files
                    .Where(file => !file.Flags.HasFlag(ManifestFileFlag.Directory) && FileCondition.Invoke(file))
                    .OrderBy(x => x.FileName)
                    .ToList();

                // Filter all files that are possible duplicates
                filteredFiles = filteredFiles.GroupBy(x => x.FileName).Select(x => x.First()).ToList();

                if (!filteredFiles.Any())
                    Logger?.LogTrace("No files to verify");

                // Verify all files in parallel
                var verificationTaskFactories = filteredFiles
                    .Select(file => new Func<CancellationToken, Task>(
                        ct => Task.Run(() => VerifyFileAsync(file, DownloadDirectory, _chunks), ct)))
                    .ToList();

                Logger?.LogTrace("Verifying files");

                await ParallelAsync(
                    _steamContentClient.MaxConcurrentDownloadsPerTask,
                    verificationTaskFactories,
                    cancellationToken);

                State = DownloadHandlerStateEnum.Verified;

                Logger?.LogTrace("Verification completed");

                if (VerificationCompleted != null)
                {
                    // Wait for all file verified eventhandlers to finish so we don't send events out of order
                    await WaitForEventHandlersAsync(cancellationToken);

                    RunEventHandler(
                        () => VerificationCompleted!.Invoke(
                            this,
                            new VerificationCompletedArgs(_chunks.Select(x => x.ManifestFile).Distinct().ToList())));
                }
            }
            catch (Exception ex)
            {
                State = DownloadHandlerStateEnum.Failed;
                Logger?.LogError(ex, "Verification failed");
                throw;
            }
        }

        public async Task DownloadAsync(CancellationToken cancellationToken = default)
        {
            if (State != DownloadHandlerStateEnum.Verified)
            {
                throw new DownloadHandlerStateMismatchException(DownloadHandlerStateEnum.Verified, State);
            }

            try
            {
                State = DownloadHandlerStateEnum.Downloading;

                Logger?.LogTrace("Creating Steam CDN server pool");

                _serverPool = new SteamCdnServerPool(_steamContentClient, AppId, cancellationToken)
                {
                    Logger = Logger
                };

                Logger?.LogTrace("Created Steam CDN server pool");

                Logger?.LogTrace("Fetching depot key");

                // Get the depot key with which we will process all chunks downloaded
                _depotKey = await _steamContentClient.GetDepotKeyAsync(DepotId, AppId);

                Logger?.LogTrace("Got depot key");

                // Download all chunks in parallel
                var sortedChunks = _chunks
                    .GroupBy(x => x.ManifestFile.FileName)
                    .OrderBy(x => x.Key)
                    .SelectMany(x => x);

                var chunkList = sortedChunks.ToList();
                Logger?.LogTrace($"Starting sequential download of {chunkList.Count} chunks");

                var taskFactoriesQueue = sortedChunks
                    .Select(chunkJob => new Func<CancellationToken, Task>(
                        ct => DownloadChunkAsync(chunkJob, ct)))
                    .ToList();

                Logger?.LogTrace("Starting {Count} download tasks", taskFactoriesQueue.Count);

                await ParallelAsync(
                    _steamContentClient.MaxConcurrentDownloadsPerTask,
                    taskFactoriesQueue,
                    cancellationToken);

                State = DownloadHandlerStateEnum.Downloaded;
                Logger?.LogTrace("Completed download tasks");

                if (DownloadComplete != null)
                {
                    // Wait for all file verified eventhandlers to finish so we don't send events out of order
                    await WaitForEventHandlersAsync(cancellationToken);

                    RunEventHandler(() => DownloadComplete!.Invoke(this, EventArgs.Empty));
                }
            }
            catch (Exception)
            {
                State = DownloadHandlerStateEnum.Failed;
                throw;
            }
            finally
            {
                if (_fileWriters.Count > 0)
                {
                    Logger?.LogTrace("Disposing all file writers");

                    await Task.WhenAll(_fileWriters.Select(x => x.DisposeAsync().AsTask()))
                        .ConfigureAwait(false);
                }

                Logger?.LogTrace("Closing server pool");
                _serverPool?.Close();
            }

            Logger?.LogTrace("Exiting");
        }

        private async Task WaitForEventHandlersAsync(CancellationToken cancellationToken = default)
        {
            while (_eventHandlersRunning > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();

                await _eventHandlerCompleted.WaitAsync(cancellationToken);
            }
        }

        private static async Task ParallelAsync(
            int maxParallel,
            IReadOnlyList<Func<CancellationToken, Task>> taskFactories,
            CancellationToken cancellationToken)
        {
            if (taskFactories.Count == 0)
                return;

            maxParallel = Math.Max(1, maxParallel);

            // Jobs get this token, so we can stop all of them if one fails
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var running = new List<Task>(maxParallel);
            var index = 0;

            try
            {
                while (index < taskFactories.Count || running.Count > 0)
                {
                    while (running.Count < maxParallel && index < taskFactories.Count)
                    {
                        running.Add(taskFactories[index++](linked.Token));
                    }

                    var completed = await Task.WhenAny(running).ConfigureAwait(false);
                    running.Remove(completed);

                    // Throws here if that job failed
                    await completed.ConfigureAwait(false);
                }
            }
            catch
            {
                // A job failed or the user cancelled: tell the others to stop and wait for them
                await linked.CancelAsync().ConfigureAwait(false);

                try
                { await Task.WhenAll(running).ConfigureAwait(false); }
                catch (Exception)
                { /* the first error is the one that gets reported */ }

                throw;
            }
        }

        // The file name comes from the manifest, which a mod author controls. Make sure the final
        // path stays inside the download folder, so a name like "..\..\x" can't write elsewhere.
        private static string GetSafeTargetPath(string directory, string fileName)
        {
            var root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(directory)) + Path.DirectorySeparatorChar;
            var full = Path.GetFullPath(Path.Combine(root, fileName));

            if (!full.StartsWith(root, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidDataException($"The manifest file name '{fileName}' points outside the download folder.");
            }

            return full;
        }

        [SuppressMessage("Security", "S4790", Justification = "Steam manifests store SHA-1 file hashes, so SHA-1 is required to compare them. It is not used for security here.")]
        [SuppressMessage("Security", "CA5350", Justification = "Same reason: the Steam manifest format fixes the hash algorithm.")]
        private static bool IsFileIdentical(string filePath, ManifestFile file)
        {
            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);

            // Different length means different file, no need to hash it
            if ((ulong) fs.Length != file.TotalSize)
                return false;

            return SHA1.HashData(fs).AsSpan().SequenceEqual(file.FileHash);
        }

        private Task VerifyFileAsync(ManifestFile file, string directory, ConcurrentBag<ChunkJob> chunks)
        {
            var filePath = GetSafeTargetPath(directory, file.FileName);

            Directory.CreateDirectory(Path.GetDirectoryName(filePath)!);

            if (File.Exists(filePath))
            {
                // If both files are identical, dont download the file
                if (IsFileIdentical(filePath, file))
                {
                    if (FileVerified != null)
                    {
                        RunEventHandler(() => FileVerified.Invoke(this, new FileVerifiedArgs(file, false)));
                    }

                    return Task.CompletedTask;
                }

                File.Delete(filePath);
            }

            // An empty file has no chunks, so nothing would ever write it. Create it here.
            if (file.TotalSize == 0)
            {
                File.WriteAllBytes(filePath, Array.Empty<byte>());

                if (FileVerified != null)
                {
                    RunEventHandler(() => FileVerified.Invoke(this, new FileVerifiedArgs(file, false)));
                }

                return Task.CompletedTask;
            }

            var target = new FileStreamTarget(filePath, file.TotalSize);

            _fileTargets.TryAdd(file, target);

            target.TotalBytes = file.TotalSize;
            target.WrittenBytes = 0;

            var fileWriter = new FileWriter(this, file, target, file.TotalSize);

            _fileWriters.Add(fileWriter);

            foreach (var chunk in file.ChunkHeaders)
            {
                chunks.Add(
                    new ChunkJob
                    {
                        ManifestFile = file,
                        Chunk = chunk,
                        FileWriter = fileWriter,
                        InternalChunk = new ChunkData
                        {
                            Checksum = chunk.Checksum,
                            ChunkID = chunk.Id,
                            CompressedLength = chunk.CompressedLength,
                            Offset = chunk.Offset,
                            UncompressedLength = chunk.UncompressedLength
                        }
                    });
            }

            if (FileVerified != null)
            {
                RunEventHandler(() => FileVerified.Invoke(this, new FileVerifiedArgs(file, true)));
            }

            return Task.CompletedTask;
        }

        private const int MaxChunkAttempts = 5;

        private async Task DownloadChunkAsync(
            ChunkJob chunkJob,
            CancellationToken cancellationToken)
        {
            var downloadedData = new byte[chunkJob.InternalChunk.UncompressedLength];
            var writtenBytes = 0;
            var downloaded = false;
            Exception? lastException = null;

            for (var attempt = 1; attempt <= MaxChunkAttempts && !downloaded; attempt++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                Server? server = null;

                try
                {
                    server = _serverPool.GetServer(cancellationToken);

                    writtenBytes = await _serverPool.CdnClient.DownloadDepotChunkAsync(
                        DepotId,
                        chunkJob.InternalChunk,
                        server,
                        downloadedData,
                        _depotKey,
                        _serverPool.DesignatedProxyServer)
                        .ConfigureAwait(false);

                    _serverPool.ReturnServer(server, false);
                    server = null;

                    downloaded = true;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    if (server != null)
                        _serverPool.ReturnServer(server, false);

                    throw;
                }
                catch (Exception ex)
                {
                    lastException = ex;

                    Logger?.LogWarning(
                        ex,
                        "Chunk download attempt {Attempt}/{Max} failed",
                        attempt,
                        MaxChunkAttempts);

                    if (server != null)
                    _serverPool.ReturnServer(server, true);

                    if (attempt < MaxChunkAttempts)
                    {
                        await Task.Delay(
                            TimeSpan.FromSeconds(attempt),
                            cancellationToken)
                        .ConfigureAwait(false);
                    }
                }
            }

            if (!downloaded)
            {
                throw new SteamDownloadException(
                    lastException ?? new InvalidOperationException(
                        "Chunk download did not succeed."));
            }

            Logger?.LogTrace("Writing to FileWriter");

            // Do not retry file writes. Disk, permission and locking errors
            // should propagate to the caller.
            await chunkJob.FileWriter.WriteAsync(
                    chunkJob.InternalChunk.Offset,
                    downloadedData[0..writtenBytes])
                .ConfigureAwait(false);

            Logger?.LogTrace("Completed downloading chunk");
        }

        private int _disposed;

        public async ValueTask DisposeAsync()
        {
            // Only the first call does the work, so disposing twice is harmless
            if (Interlocked.Exchange(ref _disposed, 1) == 1)
            {
                return;
            }

            // Release the file streams
            await Task.WhenAll(_fileWriters.Select(x => x.DisposeAsync().AsTask())).ConfigureAwait(false);

            _serverPool?.Dispose();
            _serverPool = null;
        }

        public void Dispose()
        {
            DisposeAsync().GetAwaiter().GetResult();
        }

        internal void RunEventHandler(Action action)
        {
            Interlocked.Increment(ref _eventHandlersRunning);

            _ = Task.Run(
                () =>
                {
                    try
                    {
                        action.Invoke();
                    }
                    catch (Exception ex)
                    {
                        // A broken event handler must not take the download down, but it should not vanish either
                        Logger?.LogError(ex, "An event handler threw an exception");
                    }
                    finally
                    {
                        Interlocked.Decrement(ref _eventHandlersRunning);
                        _eventHandlerCompleted.Set();
                    }
                });
        }

        internal class FileWriter : IAsyncDisposable
        {
            private readonly AsyncSemaphore _writeLock = new AsyncSemaphore(1);
            private DefaultDownloadHandler _handler;

            public ulong WrittenBytes { get; private set; }
            public ulong ExpectedBytes { get; }
            public ManifestFile ManifestFile { get; }
            public FileTarget Target { get; }
            public bool IsDone { get; private set; }

            public FileWriter(
                DefaultDownloadHandler handler,
                ManifestFile manifestFile,
                FileTarget target,
                ulong expectedBytes)
            {
                _handler = handler;
                Target = target;
                ManifestFile = manifestFile;
                ExpectedBytes = expectedBytes;
            }

            public async ValueTask DisposeAsync()
            {
                await CancelAsync();

                _handler = null;
            }

            public async Task WriteAsync(ulong offset, byte[] data)
            {
                using var semLock = await _writeLock.LockAsync().ConfigureAwait(false);

                if (IsDone) return;

                await Target.WriteAsync(offset, data);

                WrittenBytes += (ulong) data.Length;
                Target.WrittenBytes = WrittenBytes;

                if (WrittenBytes == ExpectedBytes)
                {
                    await Target.CompleteAsync().ConfigureAwait(false);

                    IsDone = true;

                    if (_handler.FileDownloaded != null)
                    {
                        _handler.RunEventHandler(() => _handler.FileDownloaded.Invoke(_handler, ManifestFile));
                    }
                }
            }

            public async Task CancelAsync()
            {
                try
                {
                    using var semLock = await _writeLock.LockAsync().ConfigureAwait(false);

                    await Target.CancelAsync().ConfigureAwait(false);

                    IsDone = true;
                }
                catch
                {
                    // ignored
                }
            }
        }

        internal class ChunkJob
        {
            public FileWriter FileWriter { get; set; }
            public ManifestFile ManifestFile { get; set; }
            public ManifestFileChunkHeader Chunk { get; set; }
            public ChunkData InternalChunk { get; set; }
        }
    }
}