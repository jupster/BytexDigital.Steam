using System;
using BytexDigital.Steam.Core.Structs;

namespace BytexDigital.Steam.ContentDelivery.Exceptions
{
    public class SteamNoContentServerFoundException : Exception
    {
        public SteamNoContentServerFoundException(
            AppId appId,
            bool chinaOnlyServersOnly = false) : base(
                chinaOnlyServersOnly
                    ? $"China-only but no eligible China-only CDN servers were found for app id = {appId}."
                    : $"No content servers could be found for app id = {appId}.")
        {
        }
    }
}
