using System;

namespace BytexDigital.Steam.ContentDelivery.Exceptions
{
    public class SteamAccessDeniedException : Exception
    {
        public SteamAccessDeniedException()
        {
        }

        public SteamAccessDeniedException(string message) : base(message)
        {
        }

        public SteamAccessDeniedException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}