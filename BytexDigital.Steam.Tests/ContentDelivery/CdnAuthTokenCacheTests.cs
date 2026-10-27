using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using BytexDigital.Steam.ContentDelivery;
using BytexDigital.Steam.Core.Structs;

namespace BytexDigital.Steam.Tests.ContentDelivery;

public class CdnAuthTokenCacheTests
{
    private static readonly DateTime Start = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DepotId Depot = new(233781);
    private const string Host = "cdn.example.com";

    private sealed class Fetcher
    {
        public int Calls;
        public CachedCdnToken? Result { get; set; }
        public Exception? Failure { get; set; }

        public Task<CachedCdnToken?> FetchAsync(CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Calls);

            if (Failure is not null)
            {
                throw Failure;
            }

            return Task.FromResult(Result);
        }
    }

    [Fact]
    public async Task NoToken_FetchesOneAndRetries()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher { Result = new CachedCdnToken("abc", Start.AddHours(1)) };

        var retry = await cache.RefreshAsync(Depot, Host, null, fetcher.FetchAsync, default);

        Assert.True(retry);
        Assert.Equal(1, fetcher.Calls);
        Assert.Equal("abc", cache.Get(Depot, Host));
    }

    [Fact]
    public async Task ValidTokenThatWasNotRejected_RetriesWithoutFetching()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher { Result = new CachedCdnToken("new", Start.AddHours(1)) };
        await cache.RefreshAsync(Depot, Host, null, fetcher.FetchAsync, default);

        // Another worker used "old" and got a 403, but "new" is already cached
        var retry = await cache.RefreshAsync(Depot, Host, "old", fetcher.FetchAsync, default);

        Assert.True(retry);
        Assert.Equal(1, fetcher.Calls);
    }

    [Fact]
    public async Task ValidTokenThatWasRejected_DropsServerWithoutFetching()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher { Result = new CachedCdnToken("abc", Start.AddHours(1)) };
        await cache.RefreshAsync(Depot, Host, null, fetcher.FetchAsync, default);

        // The server refused a token that has not expired, so a new one will not help
        var retry = await cache.RefreshAsync(Depot, Host, "abc", fetcher.FetchAsync, default);

        Assert.False(retry);
        Assert.Equal(1, fetcher.Calls);
    }

    [Fact]
    public async Task ExpiredToken_FetchesANewOne()
    {
        var now = Start;
        var cache = new CdnAuthTokenCache(() => now);
        var fetcher = new Fetcher { Result = new CachedCdnToken("first", Start.AddMinutes(30)) };
        await cache.RefreshAsync(Depot, Host, null, fetcher.FetchAsync, default);

        now = Start.AddHours(1);
        fetcher.Result = new CachedCdnToken("second", now.AddHours(1));

        var retry = await cache.RefreshAsync(Depot, Host, "first", fetcher.FetchAsync, default);

        Assert.True(retry);
        Assert.Equal(2, fetcher.Calls);
        Assert.Equal("second", cache.Get(Depot, Host));
    }

    [Fact]
    public async Task TokenAboutToExpire_CountsAsExpired()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher { Result = new CachedCdnToken("first", Start.AddSeconds(30)) };
        await cache.RefreshAsync(Depot, Host, null, fetcher.FetchAsync, default);

        fetcher.Result = new CachedCdnToken("second", Start.AddHours(1));
        await cache.RefreshAsync(Depot, Host, "first", fetcher.FetchAsync, default);

        Assert.Equal(2, fetcher.Calls);
        Assert.Equal("second", cache.Get(Depot, Host));
    }

    [Fact]
    public async Task SteamReturnsNoToken_DropsServer()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher { Result = null };

        var retry = await cache.RefreshAsync(Depot, Host, null, fetcher.FetchAsync, default);

        Assert.False(retry);
        Assert.Null(cache.Get(Depot, Host));
    }

    [Fact]
    public async Task FetchThrows_DropsServer()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher { Failure = new InvalidOperationException("Steam is down") };

        var retry = await cache.RefreshAsync(Depot, Host, null, fetcher.FetchAsync, default);

        Assert.False(retry);
    }

    [Fact]
    public async Task Cancellation_IsNotSwallowed()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher { Failure = new OperationCanceledException() };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => cache.RefreshAsync(Depot, Host, null, fetcher.FetchAsync, default));
    }

    [Fact]
    public async Task ManyWorkersFailingTogether_AskSteamOnce()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher { Result = new CachedCdnToken("abc", Start.AddHours(1)) };

        var results = await Task.WhenAll(
            Enumerable.Range(0, 20).Select(_ => cache.RefreshAsync(Depot, Host, null, fetcher.FetchAsync, default)));

        Assert.All(results, Assert.True);
        Assert.Equal(1, fetcher.Calls);
    }

    [Fact]
    public async Task TokensAreKeptPerDepotAndHost()
    {
        var cache = new CdnAuthTokenCache(() => Start);
        var fetcher = new Fetcher { Result = new CachedCdnToken("abc", Start.AddHours(1)) };
        await cache.RefreshAsync(Depot, Host, null, fetcher.FetchAsync, default);

        Assert.Null(cache.Get(Depot, "other.example.com"));
        Assert.Null(cache.Get(new DepotId(1), Host));
    }

    [Fact]
    public void GetToken_WhenMissing_ReturnsNull()
    {
        var cache = new CdnAuthTokenCache();

        Assert.Null(cache.GetToken(Key));
    }

    [Fact]
    public async Task RefreshAsync_WhenNoTokenExists_StoresReplacement()
    {
        var cache = new CdnAuthTokenCache();

        var refreshed = await cache.RefreshAsync(
            Key,
            rejectedToken: null,
            _ => Task.FromResult<CdnAuthTokenCache.Entry?>(
                Token("token-1")),
            CancellationToken.None);

        Assert.True(refreshed);
        Assert.Equal("token-1", cache.GetToken(Key));
    }

    [Fact]
    public async Task RefreshAsync_WhenCachedTokenWasRejected_RequestsReplacement()
    {
        var cache = new CdnAuthTokenCache();

        await cache.RefreshAsync(
            Key,
            rejectedToken: null,
            _ => Task.FromResult<CdnAuthTokenCache.Entry?>(
                Token("old-token")),
            CancellationToken.None);

        var refreshed = await cache.RefreshAsync(
            Key,
            rejectedToken: "old-token",
            _ => Task.FromResult<CdnAuthTokenCache.Entry?>(
                Token("new-token")),
            CancellationToken.None);

        Assert.True(refreshed);
        Assert.Equal("new-token", cache.GetToken(Key));
    }

    [Fact]
    public async Task RefreshAsync_WhenAnotherWorkerAlreadyRefreshed_ReusesToken()
    {
        var cache = new CdnAuthTokenCache();
        var providerCalled = false;

        await cache.RefreshAsync(
            Key,
            rejectedToken: null,
            _ => Task.FromResult<CdnAuthTokenCache.Entry?>(
                Token("new-token")),
            CancellationToken.None);

        var refreshed = await cache.RefreshAsync(
            Key,
            rejectedToken: "old-token",
            _ =>
            {
                providerCalled = true;
                return Task.FromResult<CdnAuthTokenCache.Entry?>(null);
            },
            CancellationToken.None);

        Assert.True(refreshed);
        Assert.False(providerCalled);
        Assert.Equal("new-token", cache.GetToken(Key));
    }

    [Fact]
    public async Task RefreshAsync_WhenProviderReturnsRejectedToken_ReturnsFalse()
    {
        var cache = new CdnAuthTokenCache();

        var refreshed = await cache.RefreshAsync(
            Key,
            rejectedToken: "rejected-token",
            _ => Task.FromResult<CdnAuthTokenCache.Entry?>(
                Token("rejected-token")),
            CancellationToken.None);

        Assert.False(refreshed);
        Assert.Null(cache.GetToken(Key));
    }

    [Fact]
    public async Task RefreshAsync_WhenProviderFails_ReturnsFalse()
    {
        var cache = new CdnAuthTokenCache();

        var refreshed = await cache.RefreshAsync(
            Key,
            rejectedToken: "old-token",
            _ => Task.FromException<CdnAuthTokenCache.Entry?>(
                new InvalidOperationException("Token request failed.")),
            CancellationToken.None);

        Assert.False(refreshed);
        Assert.Null(cache.GetToken(Key));
    }
}