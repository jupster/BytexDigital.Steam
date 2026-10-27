using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using BytexDigital.Steam.ContentDelivery.Exceptions;

namespace BytexDigital.Steam.ContentDelivery.Models.Downloading
{
    public class LegacyDownloadHandler : IDownloadHandler
    {
        // One shared client for every download. No overall timeout, because a big file can legitimately take a long time.
        private static readonly HttpClient Http = new() { Timeout = Timeout.InfiniteTimeSpan };

        public string FileUrl { get; }
        public string FileName { get; }
        public string DownloadDirectory { get; protected set; } = null!;
        public Func<ManifestFile, bool> FileCondition { get; protected set; } = null!;
        public DownloadHandlerStateEnum State { get; protected set; }

        public LegacyDownloadHandler(string fileUrl, string fileName)
        {
            FileUrl = fileUrl;
            FileName = fileName;
            State = DownloadHandlerStateEnum.Created;
        }

        public double TotalProgress { get; private set; }
        public int TotalFileCount => 1;
        public ulong TotalFileSize { get; private set; }

        public event EventHandler<ManifestFile>? FileDownloaded;
        // Legacy HTTP downloads don't perform manifest-based verification.
        public event EventHandler<FileVerifiedArgs>? FileVerified
        {
            add { }
            remove { }
        }

        public event EventHandler<VerificationCompletedArgs>? VerificationCompleted
        {
            add { }
            remove { }
        }
        public event EventHandler<EventArgs>? DownloadComplete;

        public Task SetupAsync(string directory, Func<ManifestFile, bool> condition, CancellationToken cancellationToken = default)
        {
            if (State != DownloadHandlerStateEnum.Created)
            {
                throw new DownloadHandlerStateMismatchException(DownloadHandlerStateEnum.Created, State);
            }

            DownloadDirectory = directory;
            FileCondition = condition;

            State = DownloadHandlerStateEnum.Verified;

            return Task.CompletedTask;
        }

        public Task VerifyAsync(CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public async Task DownloadAsync(CancellationToken cancellationToken = default)
        {
            string? path = null;

            try
            {
                if (State != DownloadHandlerStateEnum.Verified)
                {
                    throw new DownloadHandlerStateMismatchException(DownloadHandlerStateEnum.Verified, State);
                }

                State = DownloadHandlerStateEnum.Downloading;

                Directory.CreateDirectory(DownloadDirectory);
                path = DownloadPathHelper.GetSafePath(DownloadDirectory, FileName);

                using var response = await Http
                    .GetAsync(FileUrl, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
                    .ConfigureAwait(false);
                response.EnsureSuccessStatusCode();

                var total = response.Content.Headers.ContentLength ?? 0;
                TotalFileSize = (ulong) total;

                await using (var source = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false))
                await using (var target = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 81920, true))
                {
                    var buffer  = new byte[81920];
                    long written = 0;
                    int read;

                    while ((read = await source.ReadAsync(buffer, cancellationToken).ConfigureAwait(false)) > 0)
                    {
                        await target.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
                        written += read;

                        if (total > 0)
                            TotalProgress = (double) written / total;
                    }

                    if (total == 0)
                        TotalFileSize = (ulong) written;
                }

                TotalProgress = 1;
                State = DownloadHandlerStateEnum.Downloaded;

                var downloaded = new ManifestFile(
                    FileName,
                    new List<ManifestFileChunkHeader>(),
                    default,
                    TotalFileSize,
                    Array.Empty<byte>());

                _ = Task.Run(() => FileDownloaded?.Invoke(this, downloaded));
                _ = Task.Run(() => DownloadComplete?.Invoke(this, EventArgs.Empty));
            }
            catch
            {
                State = DownloadHandlerStateEnum.Failed;

                // Don't leave a half-written file behind
                if (path != null)
                {
                    try
                    { File.Delete(path); }
                    catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                    { /* could not clean up, the next download overwrites it anyway */ }
                }

                throw;
            }
        }

        public ValueTask DisposeAsync()
        {
            return new ValueTask(Task.CompletedTask);
        }

        public void Dispose()
        {
            DisposeAsync().GetAwaiter().GetResult();
        }
    }
}