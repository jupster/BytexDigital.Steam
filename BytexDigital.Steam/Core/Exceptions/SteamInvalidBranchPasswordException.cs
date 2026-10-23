using System;
using BytexDigital.Steam.Core.Structs;

namespace BytexDigital.Steam.ContentDelivery.Exceptions
{
    public class SteamInvalidBranchPasswordException : Exception
    {
        // The password itself is deliberately not part of the message, because messages end up in the console and log
        public SteamInvalidBranchPasswordException(
            AppId appId,
            DepotId depotId,
            string branch,
            Exception innerException = null) : base("Invalid password " +
                                                    $"for branch = {branch} of app id = {appId}, depot id = {depotId}{(innerException != null ? ". View the inner exception for more details." : "")}",
            innerException)
        {
        }
    }
}