using System;

namespace BytexDigital.Steam.ContentDelivery.Exceptions
{
    public class SteamDownloadException : Exception
    {
        public SteamDownloadException(Exception innerException) : base(
            innerException is SteamNoContentServerFoundException
                ? "Download stopped because no eligible content server was available."
                : "Failed to download at least one chunk after repeated attempts.",
            innerException)
        {
        }
    }
}