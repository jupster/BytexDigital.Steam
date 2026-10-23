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
        private const int BufferSize = 81920;

        // One shared client for every download. The default 100s timeout only covers waiting for the
        // response headers here, not the file body, so large files are fine.
        private static readonly HttpClient Http = new();

        private ManifestFile _standIn;

        public string FileUrl { get; }
        public string FileName { get; }
        public string DownloadDirectory { get; protected set; }
        public Func<ManifestFile, bool> FileCondition { get; protected set; }
        public DownloadHandlerStateEnum State { get; protected set; }

        public double TotalProgress { get; private set; }
        public int TotalFileCount => 1;
        public ulong TotalFileSize { get; private set; }

        public event EventHandler<ManifestFile> FileDownloaded;
        public event EventHandler<FileVerifiedArgs> FileVerified;
        public event EventHandler<VerificationCompletedArgs> VerificationCompleted;
        public event EventHandler<EventArgs> DownloadComplete;

        public LegacyDownloadHandler(string fileUrl, string fileName)
        {
            FileUrl = fileUrl;
            FileName = fileName;
            State = DownloadHandlerStateEnum.Created;
        }

        public Task SetupAsync(string directory, Func<ManifestFile, bool> condition, CancellationToken cancellationToken = default)
        {
            if (State != DownloadHandlerStateEnum.Created)
            {
                throw new DownloadHandlerStateMismatchException(DownloadHandlerStateEnum.Created, State);
            }

            DownloadDirectory = directory;
            FileCondition = condition;

            // Fail early if the name from Steam is unusable
            _standIn = new ManifestFile(GetSafeFileName(), new List<ManifestFileChunkHeader>(), default, 0, Array.Empty<byte>());

            State = DownloadHandlerStateEnum.SetUp;
            return Task.CompletedTask;
        }

        public Task VerifyAsync(CancellationToken cancellationToken = default)
        {
            if (State != DownloadHandlerStateEnum.SetUp)
            {
                throw new DownloadHandlerStateMismatchException(DownloadHandlerStateEnum.SetUp, State);
            }

            // There is nothing on disk to compare against, so the single file always needs downloading
            State = DownloadHandlerStateEnum.Verified;
            FileVerified?.Invoke(this, new FileVerifiedArgs(_standIn, true));
            VerificationCompleted?.Invoke(this, new VerificationCompletedArgs(new List<ManifestFile> { _standIn }));

            return Task.CompletedTask;
        }

        public async Task DownloadAsync(CancellationToken cancellationToken = default)
        {
            if (State != DownloadHandlerStateEnum.Verified)
            {
                throw new DownloadHandlerStateMismatchException(DownloadHandlerStateEnum.Verified, State);
            }

            State = DownloadHandlerStateEnum.Downloading;
            string targetPath = null;

            try
            {
                Directory.CreateDirectory(DownloadDirectory);
                targetPath = Path.Combine(DownloadDirectory, GetSafeFileName());

                using var response = await Http.GetAsync(FileUrl, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
                    .ConfigureAwait(false);
                response.EnsureSuccessStatusCode();

                TotalFileSize = (ulong) (response.Content.Headers.ContentLength ?? 0);

                await using var source = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
                await using var target = new FileStream(targetPath, FileMode.Create, FileAccess.Write, FileShare.None, BufferSize, true);

                var buffer = new byte[BufferSize];
                ulong downloaded = 0;
                int read;

                while ((read = await source.ReadAsync(buffer.AsMemory(), cancellationToken).ConfigureAwait(false)) > 0)
                {
                    await target.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
                    downloaded += (ulong) read;

                    if (TotalFileSize > 0)
                        TotalProgress = (double) downloaded / TotalFileSize;
                }

                TotalFileSize = downloaded;
                TotalProgress = 1;
            }
            catch (Exception)
            {
                State = DownloadHandlerStateEnum.Failed;

                // Don't leave a half-written file that looks like a finished one
                try
                {
                    if (targetPath != null && File.Exists(targetPath))
                        File.Delete(targetPath);
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                { /* the original error matters more */ }

                throw;
            }

            State = DownloadHandlerStateEnum.Downloaded;

            var done = new ManifestFile(_standIn.FileName, new List<ManifestFileChunkHeader>(), default, TotalFileSize, Array.Empty<byte>());
            FileDownloaded?.Invoke(this, done);
            DownloadComplete?.Invoke(this, EventArgs.Empty);
        }

        // The file name comes from the mod author, so it can't be trusted as a path.
        // Keep only the last part, so "..\..\x" or "C:\x" can't escape the download folder.
        private string GetSafeFileName()
        {
            var name = Path.GetFileName((FileName ?? string.Empty).Replace('\\', '/'));

            if (string.IsNullOrWhiteSpace(name))
                throw new InvalidOperationException($"The Workshop item has no usable file name ('{FileName}').");

            return name;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        public void Dispose()
        {
            // Nothing to release: the HttpClient is shared and the streams are closed after each download
        }
    }
}