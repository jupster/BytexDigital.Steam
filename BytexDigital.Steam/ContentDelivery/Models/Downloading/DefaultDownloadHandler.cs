using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
        internal byte[]? _depotKey;
        internal AsyncAutoResetEvent _eventHandlerCompleted = new AsyncAutoResetEvent(true);
        internal int _eventHandlersRunning;

        private ConcurrentDictionary<ManifestFile, FileTarget>? _fileTargets =
            new ConcurrentDictionary<ManifestFile, FileTarget>();

        private ConcurrentBag<FileWriter>? _fileWriters = new ConcurrentBag<FileWriter>();
        private SteamCdnServerPool? _serverPool;
        private ConcurrentBag<ChunkJob> _chunks = new ConcurrentBag<ChunkJob>();
        internal SteamContentClient? _steamContentClient;

        private ConcurrentDictionary<ManifestFile, FileTarget> FileTargets =>
            _fileTargets ?? throw new ObjectDisposedException(nameof(DefaultDownloadHandler));

        private ConcurrentBag<FileWriter> FileWriters =>
            _fileWriters ?? throw new ObjectDisposedException(nameof(DefaultDownloadHandler));

        private SteamContentClient ContentClient =>
            _steamContentClient ?? throw new ObjectDisposedException(nameof(DefaultDownloadHandler));

        public Manifest Manifest { get; }
        public AppId AppId { get; }
        public DepotId DepotId { get; }
        public ManifestId ManifestId { get; }
        public DownloadHandlerStateEnum State { get; protected set; }
	public string DownloadDirectory { get; protected set; } = null!;
        public Func<ManifestFile, bool> FileCondition { get; protected set; } = null!;

        public ILogger? Logger { get; set; }

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

        public event EventHandler<ManifestFile>? FileDownloaded;
        public event EventHandler<FileVerifiedArgs>? FileVerified;
        public event EventHandler<VerificationCompletedArgs>? VerificationCompleted;
        public event EventHandler<EventArgs>? DownloadComplete;

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
                    return FileTargets.Count;
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
                    return FileTargets.Select(x => x.Key.TotalSize).Aggregate(0UL, (a, b) => a + b);
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
                    var totalBytes = FileTargets.Sum(x => (long) x.Value.TotalBytes);
                    var currentBytes = FileTargets.Sum(x => (long) x.Value.WrittenBytes);

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
                {
                    State = DownloadHandlerStateEnum.Verified;
                    Logger?.LogTrace("No files to download");

                    if (VerificationCompleted != null)
                    {
                        await WaitForEventHandlersAsync(cancellationToken);

                        RunEventHandler(
                            () => VerificationCompleted!.Invoke(
                                this,
                                new VerificationCompletedArgs(new List<ManifestFile>())));
                    }

                    return;
                }

                // Verify all files in parallel
                var verificationTaskFactories =
                    filteredFiles.Select(
                        file => new Func<Task>(
                            async () => await Task.Run(() => VerifyFileAsync(file, DownloadDirectory, _chunks))));

                Logger?.LogTrace("Verifying files");

                await ParallelAsync(
                    ContentClient.MaxConcurrentDownloadsPerTask,
                    verificationTaskFactories,
                    cancellationToken);

                State = DownloadHandlerStateEnum.Verified;

                Logger?.LogTrace("Verification completed");

                // Always wait, so every FileVerified handler has finished before anyone reads the results
                await WaitForEventHandlersAsync(cancellationToken);

                if (VerificationCompleted != null)
                {
                    RunEventHandler(
                        () => VerificationCompleted!.Invoke(
                            this,
                            new VerificationCompletedArgs(FileWriters.Select(x => x.ManifestFile).ToList())));
                }
            }
            catch
            {
                State = DownloadHandlerStateEnum.Failed;
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

                // Zero-byte files have no chunks, so create them up front
                foreach (var fileWriter in FileWriters.Where(x => x.ExpectedBytes == 0))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    await fileWriter.WriteAsync(0, Array.Empty<byte>())
                        .ConfigureAwait(false);
                }

                if (_chunks.IsEmpty)
                {
                    Logger?.LogTrace("No chunks to download");

                    State = DownloadHandlerStateEnum.Downloaded;

                    if (DownloadComplete != null)
                    {
                        await WaitForEventHandlersAsync(cancellationToken);

                        RunEventHandler(
                            () => DownloadComplete!.Invoke(this, EventArgs.Empty));
                    }

                    return;
                }

                Logger?.LogTrace("Creating Steam CDN server pool");

                _serverPool = new SteamCdnServerPool(ContentClient, AppId, cancellationToken)
                {
                    Logger = Logger
                };

                Logger?.LogTrace("Created Steam CDN server pool");

                Logger?.LogTrace("Fetching depot key");

                // Get the depot key with which we will process all chunks downloaded
                _depotKey = await ContentClient.GetDepotKeyAsync(DepotId, AppId);

                Logger?.LogTrace("Got depot key");

                // Download all chunks in parallel
                var sortedChunks = _chunks
                    .GroupBy(x => x.ManifestFile.FileName)
                    .OrderBy(x => x.Key)
                    .SelectMany(x => x);

                var chunkList = sortedChunks.ToList();
                Logger?.LogTrace($"Starting sequential download of {chunkList.Count} chunks");

                var taskFactoriesQueue = chunkList.Select(
                        chunkJob =>
                            new Func<Task>(
                                async () => await Task.Run(
                                    () => DownloadChunkAsync(chunkJob, cancellationToken))))
                    .ToList();

                Logger?.LogTrace($"Starting {taskFactoriesQueue.Count} download tasks");

                if (taskFactoriesQueue.Count > 0)
                {
                    await ParallelAsync(
                        ContentClient.MaxConcurrentDownloadsPerTask,
                        taskFactoriesQueue,
                        cancellationToken);
                }

                State = DownloadHandlerStateEnum.Downloaded;
                Logger?.LogTrace("Completed download tasks");

                if (DownloadComplete != null)
                {
                    // Wait for all file verified eventhandlers to finish so we don't send events out of order
                    await WaitForEventHandlersAsync(cancellationToken);

                    RunEventHandler(() => DownloadComplete!.Invoke(this, EventArgs.Empty));
                }
            }
            catch
            {
                State = DownloadHandlerStateEnum.Failed;
                throw;
            }
            finally
            {
                try
                {
                    if (FileWriters.Count > 0)
                    {
                        Logger?.LogTrace("Disposing all file writers");

                        await Task.WhenAll(
                            FileWriters.Select(x => x.DisposeAsync().AsTask()));
                    }
                }
                finally
                {
                    Logger?.LogTrace("Closing server pool");

                    _serverPool?.Dispose();
                    _serverPool = null;
                }
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

        private async Task ParallelAsync(
            int maxParallel,
            IEnumerable<Func<Task>> taskFactories,
            CancellationToken cancellationToken)
        {
            var taskFactoriesSource = taskFactories.ToArray();

            if (taskFactoriesSource.Length == 0)
            {
                return;
            }

            if (maxParallel <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxParallel));
            }

            var tasksRunning = new List<Task>(50);
            var index = 0;

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                while (tasksRunning.Count < maxParallel && index < taskFactoriesSource.Length)
                {
                    var taskFactory = taskFactoriesSource[index++];

                    tasksRunning.Add(taskFactory());
                }

                var completedTask = await Task.WhenAny(tasksRunning).ConfigureAwait(false);

                await completedTask.ConfigureAwait(false);

                tasksRunning.Remove(completedTask);
            } while (index < taskFactoriesSource.Length || tasksRunning.Count != 0);
        }

        private Task VerifyFileAsync(ManifestFile file, string directory, ConcurrentBag<ChunkJob> chunks)
        {
            var filePath = DownloadPathHelper.GetSafePath(
                directory,
                file.FileName);
            var fileDirectory = Path.GetDirectoryName(filePath) ?? directory;

            Directory.CreateDirectory(fileDirectory);

            if (File.Exists(filePath))
            {
                using (var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    // If the files are the same length, verify them using their hash
                    if ((ulong) fs.Length == file.TotalSize)
                    {
                        using var bs = new BufferedStream(fs);

                        // If both files are identical, dont download the file
                        if (SHA1.HashData(bs).SequenceEqual(file.FileHash))
                        {
                            if (FileVerified != null)
                            {
                                RunEventHandler(() => FileVerified.Invoke(this, new FileVerifiedArgs(file, false)));
                            }

                            return Task.CompletedTask;
                        }
                    }
                }

                File.Delete(filePath);
            }

            var target = new FileStreamTarget(filePath, file.TotalSize);

            FileTargets.TryAdd(file, target);

            target.TotalBytes = file.TotalSize;
            target.WrittenBytes = 0;

            var fileWriter = new FileWriter(this, file, target, file.TotalSize);

            FileWriters.Add(fileWriter);

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

        private async Task DownloadChunkAsync(ChunkJob chunkJob, CancellationToken cancellationToken)
        {
        const int maxAttempts = 3;
		    var serverPool = _serverPool
                ?? throw new InvalidOperationException(
                    "The CDN server pool has not been initialized or is no longer available.");

        var depotKey = _depotKey
            ?? throw new InvalidOperationException(
                "The depot key has not been initialized. SetupAsync must complete before downloading chunks.");

        var downloadedData = new byte[chunkJob.InternalChunk.UncompressedLength];
        var writtenBytes = 0;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            Server? server = null;

            try
            {
                server = serverPool.GetServer(cancellationToken);

                writtenBytes = await _serverPool.CdnClient.DownloadDepotChunkAsync(
                    DepotId,
                    chunkJob.InternalChunk,
                    server,
                    downloadedData,
                    depotKey,
                    serverPool.DesignatedProxyServer)
                    .ConfigureAwait(false);

                serverPool.ReturnServer(server, false);
                server = null;

                break;
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                if (server != null)
                {
                    _serverPool.ReturnServer(server, false);
                }

                throw;
            }
            catch (Exception ex)
            {
                if (server != null)
                {
                    serverPool.ReturnServer(server, true);
                }

                Logger?.LogWarning(
                    ex,
                    "Attempt {Attempt} of {MaxAttempts} failed for chunk {ChunkId} in file {FileName}.",
                    attempt,
                    maxAttempts,
                    chunkJob.InternalChunk.ChunkID,
                    chunkJob.ManifestFile.FileName);

                if (attempt == maxAttempts)
                {
                    throw new SteamDownloadException(ex);
                }

                await Task.Delay(
                    TimeSpan.FromMilliseconds(250 * attempt),
                    cancellationToken).ConfigureAwait(false);
            }
        }

        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            await chunkJob.FileWriter.WriteAsync(
                chunkJob.InternalChunk.Offset,
                downloadedData[0..writtenBytes])
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            Logger?.LogError(
                ex,
                "Failed to write chunk {ChunkId} to file {FileName}.",
                chunkJob.InternalChunk.ChunkID,
                chunkJob.ManifestFile.FileName);

            throw;
        }

        Logger?.LogTrace("Completed downloading chunk");
    }

        public async ValueTask DisposeAsync()
        {
            // Already disposed
            var fileWriters = _fileWriters;

            if (fileWriters == null)
                return;

            // Dispose all file writers to release potential file streams
            await Task.WhenAll(fileWriters.Select(x => Task.Run(async () => await x.DisposeAsync().AsTask())));

            _fileTargets = null;
            _fileWriters = null;
            _steamContentClient = null;

            // Dispose the server pool
            try
            {
                _serverPool?.Dispose();
            }
            catch
            {
                // ignored
            }

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
            private DefaultDownloadHandler? _handler;

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

                    var handler = _handler;
                    if (handler?.FileDownloaded is { } fileDownloaded)
                    {
                        handler.RunEventHandler(() => fileDownloaded.Invoke(handler, ManifestFile));
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
            public required FileWriter FileWriter { get; set; }
            public required ManifestFile ManifestFile { get; set; }
            public required ManifestFileChunkHeader Chunk { get; set; }
            public required ChunkData InternalChunk { get; set; }
        }
    }
}