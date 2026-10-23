using System;
using System.Runtime.InteropServices;

namespace BytexDigital.Steam.Core.Enumerations
{
    public sealed record SteamOs
    {
        public static readonly SteamOs Windows = new("windows");
        public static readonly SteamOs Linux = new("linux");
        public static readonly SteamOs MacOs = new("macos");

        public string Identifier { get; }

        public SteamOs(string identifier)
        {
            Identifier = identifier ??
                         throw new ArgumentNullException(nameof(identifier), "The OS identifier is not valid.");
        }

        public static implicit operator SteamOs(string identifier)
        {
            return new SteamOs(identifier);
        }

        public static SteamOs GetCurrent()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return Windows;
            }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                return Linux;
            }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                return MacOs;
            }

            return new SteamOs("unknown");
        }

        public override string ToString()
        {
            return Identifier;
        }
    }
}