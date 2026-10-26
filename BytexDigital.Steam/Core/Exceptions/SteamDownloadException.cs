using System;

namespace BytexDigital.Steam.ContentDelivery.Exceptions
{
    public class SteamDownloadException : Exception
    {
        public SteamDownloadException(Exception innerException) : base(
            "Failed to download at least one chunk repeatedly.", innerException)
        {
        }
    }
}
