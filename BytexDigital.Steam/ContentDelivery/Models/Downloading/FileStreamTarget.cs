using System;
using System.IO;
using System.Threading.Tasks;

namespace BytexDigital.Steam.ContentDelivery.Models.Downloading
{
    public class FileStreamTarget : FileTarget
    {
        private readonly string _filePath;
        private readonly ulong _fileSize;
        public FileStream? FileStream { get; private set; }

        public FileStreamTarget(string filePath, ulong fileSize)
        {
            _filePath = filePath;
            _fileSize = fileSize;
        }
        
        public override async Task CompleteAsync()
        {
            var fileStream = FileStream
                ?? throw new InvalidOperationException(
                    "Cannot complete the file before its stream has been opened.");

            await fileStream.FlushAsync().ConfigureAwait(false);
            fileStream.Close();
        }

        public override async Task WriteAsync(ulong offset, byte[] data)
        {
            var fileStream = FileStream;

            if (fileStream == null)
            {
			    fileStream = new FileStream(
                    _filePath,
                    FileMode.Create,
                    FileAccess.Write,
                    FileShare.None,
                    131072,
                    true);

                FileStream = fileStream;
                fileStream.SetLength((long)_fileSize);
            }

        fileStream.Seek((long)offset, SeekOrigin.Begin);
        await fileStream.WriteAsync(data.AsMemory()).ConfigureAwait(false);
    }

        public override async Task CancelAsync()
        {
            var fileStream = FileStream;

            // Nothing was written yet, so there is no stream to close.
            if (fileStream == null)
            {
                return;
            }

            try
            {
                await fileStream.FlushAsync().ConfigureAwait(false);
                fileStream.Close();
            }
            catch (Exception ex) when (ex is IOException or ObjectDisposedException)
            {
                // Already closed, or the disk is gone. We are cancelling anyway.
            }
        }
        public override async ValueTask DisposeAsync()
        {
            await CancelAsync();
        }
    }
}
