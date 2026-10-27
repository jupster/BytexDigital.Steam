using System.Collections.Concurrent;
using BytexDigital.Steam.Core.Structs;
using Nito.AsyncEx;

namespace BytexDigital.Steam.ContentDelivery
{
    internal readonly record struct CachedCdnToken(string Value, DateTime Expiration);

    /// <summary>
    ///     Holds CDN auth tokens, one per depot and host, and decides when a 403 is worth retrying.
    /// </summary>
    internal sealed class CdnAuthTokenCache
    {
        // A token this close to expiring is treated as already expired
        private static readonly TimeSpan ExpiryMargin = TimeSpan.FromMinutes(1);

        private readonly ConcurrentDictionary<string, CachedCdnToken> _tokens = new();
        private readonly AsyncLock _lock = new();
        private readonly Func<DateTime> _utcNow;

        public CdnAuthTokenCache(Func<DateTime>? utcNow = null)
        {
            _utcNow = utcNow ?? (() => DateTime.UtcNow);
        }

        private static string Key(DepotId depotId, string host) => $"{depotId.Id}:{host}";

        // The cached token for this depot on this host, or null if the server never asked for one
        public string? Get(DepotId depotId, string host)
        {
            return _tokens.TryGetValue(Key(depotId, host), out var token) ? token.Value : null;
        }

        /// <summary>
        ///     Called after a CDN server answered 403. Returns true if retrying on this server is worthwhile,
        ///     false if the server should be dropped.
        /// </summary>
        public async Task<bool> RefreshAsync(
            DepotId depotId,
            string host,
            string? rejectedToken,
            Func<CancellationToken, Task<CachedCdnToken?>> fetch,
            CancellationToken cancellationToken)
        {
            var key = Key(depotId, host);

            // One request at a time, so many workers failing together only ask Steam once
            using var lk = await _lock.LockAsync(cancellationToken);

            if (_tokens.TryGetValue(key, out var existing) && existing.Expiration > _utcNow() + ExpiryMargin)
            {
                // A valid token that is not the rejected one means another worker already refreshed it.
                // The rejected token itself still being valid means the token is not the problem: this server refuses us.
                return existing.Value != rejectedToken;
            }

            try
            {
                var fetched = await fetch(cancellationToken);

                if (fetched is null)
                {
                    return false;
                }

                _tokens[key] = fetched.Value;
                return true;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return false;
            }
        }
    }
}