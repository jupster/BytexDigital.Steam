using System;
using System.IO;

namespace BytexDigital.Steam.ContentDelivery.Models.Downloading
{
    internal static class DownloadPathHelper
    {
        public static string GetSafePath(string directory, string fileName)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(directory);
            ArgumentException.ThrowIfNullOrWhiteSpace(fileName);

            var fullDirectory = Path.GetFullPath(directory);
            var fullFilePath = Path.GetFullPath(
                Path.Combine(fullDirectory, fileName));

            var comparison = OperatingSystem.IsWindows()
                ? StringComparison.OrdinalIgnoreCase
                : StringComparison.Ordinal;

            var directoryPrefix = Path.EndsInDirectorySeparator(fullDirectory)
                ? fullDirectory
                : fullDirectory + Path.DirectorySeparatorChar;

            if (string.Equals(fullFilePath, fullDirectory, comparison) ||
                !fullFilePath.StartsWith(directoryPrefix, comparison))
            {
                throw new InvalidDataException(
                    $"File name '{fileName}' resolves outside the download directory.");
            }

            return fullFilePath;
        }
    }
}