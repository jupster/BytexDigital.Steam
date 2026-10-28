using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace BytexDigital.Steam.ContentDelivery;

internal sealed class CdnAuthTokenCache
{
    private static readonly TimeSpan ExpirySafetyWindow =
        TimeSpan.FromMinutes(1);

    private readonly ConcurrentDictionary<string, Entry> _tokens = new();
    private readonly SemaphoreSlim _refreshLock = new(1, 1);
    private readonly Func<DateTime> _utcNow;

    internal CdnAuthTokenCache(Func<DateTime>? utcNow = null)
    {
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
    }

    /// <summary>
    /// Gets a usable token for the specified cache key.
    /// Returns null if no usable token is cached.
    /// </summary>
    internal string? GetToken(string key)
    {
        return _tokens.TryGetValue(key, out var entry) && IsUsable(entry)
            ? entry.Token
            : null;
    }

    internal async Task<bool> InvalidateIfTokenMatchesAsync(
        string key,
        string? rejectedToken,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);

        if (string.IsNullOrWhiteSpace(rejectedToken))
        {
            return false;
        }

        await _refreshLock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (_tokens.TryGetValue(key, out var existing) &&
                string.Equals(
                    existing.Token,
                    rejectedToken,
                    StringComparison.Ordinal))
            {
                return _tokens.TryRemove(key, out _);
            }

            // A newer token may have replaced the rejected one.
            return false;
        }
        finally
        {
            _refreshLock.Release();
        }
    }

    /// <summary>
    /// Attempts to refresh a CDN authentication token after a failed request.
    /// Returns true when a usable replacement token is available.
    /// Returns false when the server should not be retried with this cache state.
    /// </summary>
    internal async Task<bool> RefreshAsync(
        string key,
        string? rejectedToken,
        Func<CancellationToken, Task<Entry?>> requestToken,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(requestToken);

        await _refreshLock
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);

        try
        {
            if (_tokens.TryGetValue(key, out var existing))
            {
                // Another worker may already have refreshed the rejected token.
                if (IsUsable(existing) &&
                    !string.Equals(
                        existing.Token,
                        rejectedToken,
                        StringComparison.Ordinal))
                {
                    return true;
                }

                // Remove an expired token or the token that was rejected.
                _tokens.TryRemove(key, out _);
            }

            Entry? replacement;

            try
            {
                replacement = await requestToken(cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch
            {
                // Refresh failed; the caller can try another server.
                return false;
            }

            // Never retry with a missing, expired, nearly expired,
            // or unchanged replacement token.
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

    private bool IsUsable(Entry entry)
    {
        return !string.IsNullOrWhiteSpace(entry.Token) &&
               entry.Expiration > _utcNow().Add(ExpirySafetyWindow);
    }

    internal sealed record Entry(string? Token, DateTime Expiration);
}