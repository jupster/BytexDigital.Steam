using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace BytexDigital.Steam.ContentDelivery;

internal sealed class CdnAuthTokenCache
{
    private static readonly TimeSpan ExpirySafetyWindow =
        TimeSpan.FromMinutes(1);

    /// <summary>
    ///     Holds CDN auth tokens, one per depot and host, and decides when a 403 is worth retrying.
    /// </summary>
        // A token this close to expiring is treated as already expired
    private readonly ConcurrentDictionary<string, Entry> _tokens = new();
    private readonly SemaphoreSlim _refreshLock = new(1, 1);

    internal string? GetToken(string key)
    {
        return _tokens.TryGetValue(key, out var entry) && IsUsable(entry)
            ? entry.Token
            : null;
    }

    internal async Task<bool> RefreshAsync(
        string key,
        string? rejectedToken,
        Func<CancellationToken, Task<Entry?>> requestToken,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(requestToken);

        await _refreshLock.WaitAsync(cancellationToken).ConfigureAwait(false);

        // The cached token for this depot on this host, or null if the server never asked for one
        try
        {
            if (_tokens.TryGetValue(key, out var existing))
            {
                // Another worker has already replaced the rejected token.
                if (IsUsable(existing) &&
                    !string.Equals(
                        existing.Token,
                        rejectedToken,
                        StringComparison.Ordinal))
                {
                    return true;
                }

        /// <summary>
        ///     Called after a CDN server answered 403. Returns true if retrying on this server is worthwhile,
        ///     false if the server should be dropped.
        /// </summary>
                _tokens.TryRemove(key, out _);
            }

            // One request at a time, so many workers failing together only ask Steam once
            Entry? replacement;

            try
            {
                // A valid token that is not the rejected one means another worker already refreshed it.
                // The rejected token itself still being valid means the token is not the problem: this server refuses us.
                replacement = await requestToken(cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch
            {
                return false;
            }

            if (replacement is null ||
                string.IsNullOrWhiteSpace(replacement.Token) ||
                !IsUsable(replacement) ||
                string.Equals(
                    replacement.Token,
                    rejectedToken,
                    StringComparison.Ordinal))
            {
                return false;
            }

            _tokens[key] = replacement;
            return true;
        }
        finally
        {
            _refreshLock.Release();
        }
    }

    private static bool IsUsable(Entry entry)
    {
        return !string.IsNullOrWhiteSpace(entry.Token) &&
               entry.Expiration > DateTime.UtcNow.Add(ExpirySafetyWindow);
    }

    internal sealed record Entry(string? Token, DateTime Expiration);
}
