using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using BytexDigital.Steam.ContentDelivery;

namespace BytexDigital.Steam.Tests.ContentDelivery;

public class CdnAuthTokenCacheTests
{
    private static readonly DateTime Start =
        new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    private const string Key = "233781:cdn.example.com";

    private static CdnAuthTokenCache.Entry Token(
        string token,
        DateTime? expiration = null)
    {
        return new CdnAuthTokenCache.Entry(
            token,
            expiration ?? Start.AddHours(1));
    }

    private sealed class Fetcher
    {
        public int Calls;
        public CdnAuthTokenCache.Entry? Result;
        public Exception? Failure;
        public TimeSpan Delay;

        public async Task<CdnAuthTokenCache.Entry?> FetchAsync(
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Calls);

            if (Delay > TimeSpan.Zero)
            {
                await Task.Delay(Delay, cancellationToken);
            }

            var failure = Failure;

            if (failure is not null)
            {
                throw failure;
            }

            return Result;
        }
    }

    [Fact]
    public void GetToken_WhenMissing_ReturnsNull()
    {
        var cache = new CdnAuthTokenCache(() => Start);

        Assert.Null(cache.GetToken(Key));
    }

    [Fact]
    public async Task RefreshAsync_WhenNoTokenExists_StoresReplacement()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher { Result = Token("token-1") };

        var refreshed = await cache.RefreshAsync(
            Key, null, fetcher.FetchAsync, CancellationToken.None);

        Assert.True(refreshed);
        Assert.Equal(1, fetcher.Calls);
        Assert.Equal("token-1", cache.GetToken(Key));
    }

    [Fact]
    public async Task RefreshAsync_WhenCachedTokenWasRejected_RequestsReplacement()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher { Result = Token("old-token") };

        var initialRefresh = await cache.RefreshAsync(
            Key, null, fetcher.FetchAsync, CancellationToken.None);

        Assert.True(initialRefresh);

        fetcher.Result = Token("new-token");

        var refreshed = await cache.RefreshAsync(
            Key, "old-token", fetcher.FetchAsync, CancellationToken.None);

        Assert.True(refreshed);
        Assert.Equal(2, fetcher.Calls);
        Assert.Equal("new-token", cache.GetToken(Key));
    }

    [Fact]
    public async Task RefreshAsync_WhenAnotherWorkerAlreadyRefreshed_ReusesToken()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher { Result = Token("new-token") };

        var initialRefresh = await cache.RefreshAsync(
            Key, null, fetcher.FetchAsync, CancellationToken.None);

        Assert.True(initialRefresh);

        var refreshed = await cache.RefreshAsync(
            Key,
            "old-token",
            _ => throw new InvalidOperationException(
                "A cached replacement should have been reused."),
            CancellationToken.None);

        Assert.True(refreshed);
        Assert.Equal(1, fetcher.Calls);
        Assert.Equal("new-token", cache.GetToken(Key));
    }

    [Fact]
    public async Task RefreshAsync_WhenProviderReturnsRejectedToken_ReturnsFalse()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher { Result = Token("rejected-token") };

        var refreshed = await cache.RefreshAsync(
            Key,
            "rejected-token",
            fetcher.FetchAsync,
            CancellationToken.None);

        Assert.False(refreshed);
        Assert.Null(cache.GetToken(Key));
    }

    [Fact]
    public async Task RefreshAsync_WhenProviderReturnsNoToken_ReturnsFalse()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher { Result = null };

        var refreshed = await cache.RefreshAsync(
            Key, null, fetcher.FetchAsync, CancellationToken.None);

        Assert.False(refreshed);
        Assert.Null(cache.GetToken(Key));
    }

    [Fact]
    public async Task RefreshAsync_WhenProviderFails_ReturnsFalse()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher
        {
            Failure = new InvalidOperationException("Token request failed.")
        };

        var refreshed = await cache.RefreshAsync(
            Key, null, fetcher.FetchAsync, CancellationToken.None);

        Assert.False(refreshed);
        Assert.Null(cache.GetToken(Key));
    }

    [Fact]
    public async Task RefreshAsync_DoesNotSwallowCancellation()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher
        {
            Failure = new OperationCanceledException()
        };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => cache.RefreshAsync(
                Key, null, fetcher.FetchAsync, CancellationToken.None));
    }

    [Fact]
    public async Task RefreshAsync_WhenTokenExpires_RequestsReplacement()
    {
        var now = Start;
        var cache = new CdnAuthTokenCache(() => now);
        var fetcher = new Fetcher
        {
            Result = Token("old-token", Start.AddHours(1))
        };

        var initialRefresh = await cache.RefreshAsync(
            Key, null, fetcher.FetchAsync, CancellationToken.None);

        Assert.True(initialRefresh);

        now = Start.AddHours(2);
        fetcher.Result = Token("new-token", now.AddHours(1));

        Assert.Null(cache.GetToken(Key));

        var refreshed = await cache.RefreshAsync(
            Key, "old-token", fetcher.FetchAsync, CancellationToken.None);

        Assert.True(refreshed);
        Assert.Equal(2, fetcher.Calls);
        Assert.Equal("new-token", cache.GetToken(Key));
    }

    [Fact]
    public async Task RefreshAsync_WhenReplacementIsNearlyExpired_ReturnsFalse()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher
        {
            Result = Token("short-lived", Start.AddSeconds(30))
        };

        var refreshed = await cache.RefreshAsync(
            Key, null, fetcher.FetchAsync, CancellationToken.None);

        Assert.False(refreshed);
        Assert.Null(cache.GetToken(Key));
    }

    [Fact]
    public async Task RefreshAsync_WhenManyWorkersFailTogether_RequestsOneToken()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher
        {
            Result = Token("shared-token"),
            Delay = TimeSpan.FromMilliseconds(20)
        };

        var results = await Task.WhenAll(
            Enumerable.Range(0, 20)
                .Select(_ => cache.RefreshAsync(
                    Key, null, fetcher.FetchAsync, CancellationToken.None)));

        Assert.All(results, result => Assert.True(result));
        Assert.Equal(1, fetcher.Calls);
        Assert.Equal("shared-token", cache.GetToken(Key));
    }

    [Fact]
    public async Task Tokens_AreKeptSeparateByKey()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher { Result = Token("token-1") };

        var refreshed = await cache.RefreshAsync(
            Key, null, fetcher.FetchAsync, CancellationToken.None);

        Assert.True(refreshed);
        Assert.Equal("token-1", cache.GetToken(Key));
        Assert.Null(cache.GetToken("233781:other.example.com"));
        Assert.Null(cache.GetToken("1:cdn.example.com"));
    }

    [Fact]
    public async Task InvalidateIfTokenMatches_RemovesRejectedToken()
    {
        var cache = new CdnAuthTokenCache(() => Start);

        await cache.RefreshAsync(
            Key,
            null,
            _ => Task.FromResult<CdnAuthTokenCache.Entry?>(
                Token("rejected-token")),
            CancellationToken.None);

        var removed = await cache.InvalidateIfTokenMatchesAsync(
            Key,
            "rejected-token",
            CancellationToken.None);

        Assert.True(removed);
        Assert.Null(cache.GetToken(Key));
    }

    [Fact]
    public async Task InvalidateIfTokenMatches_PreservesDifferentNewerToken()
    {
        var cache = new CdnAuthTokenCache(() => Start);

        await cache.RefreshAsync(
            Key,
            null,
            _ => Task.FromResult<CdnAuthTokenCache.Entry?>(
                Token("newer-token")),
            CancellationToken.None);

        var removed = await cache.InvalidateIfTokenMatchesAsync(
            Key,
            "older-rejected-token",
            CancellationToken.None);

        Assert.False(removed);
        Assert.Equal("newer-token", cache.GetToken(Key));
    }

        [Fact]
    public async Task RefreshAsync_DifferentKeysDoNotWaitForEachOther()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var release = new TaskCompletionSource<CdnAuthTokenCache.Entry?>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        // This request stays in flight until the test lets it finish
        var slow = cache.RefreshAsync(
            "233781:slow.example.com", null, _ => release.Task, CancellationToken.None);

        // With one shared lock this would hang and time out
        var fast = await cache.RefreshAsync(
                "233781:fast.example.com",
                null,
                _ => Task.FromResult<CdnAuthTokenCache.Entry?>(Token("fast-token")),
                CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));

        Assert.True(fast);
        Assert.False(slow.IsCompleted);
        Assert.Equal("fast-token", cache.GetToken("233781:fast.example.com"));

        release.SetResult(Token("slow-token"));

        Assert.True(await slow);
        Assert.Equal("slow-token", cache.GetToken("233781:slow.example.com"));
    }
}