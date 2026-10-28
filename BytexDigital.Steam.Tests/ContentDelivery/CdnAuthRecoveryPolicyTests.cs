using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using BytexDigital.Steam.ContentDelivery;

namespace BytexDigital.Steam.Tests.ContentDelivery;

public class CdnAuthRecoveryPolicyTests
{
    [Fact]
    public async Task FirstCdn403_RefreshSucceeds_RetriesSameServer()
    {
        var refreshCalls = 0;
        var invalidateCalls = 0;

        var action = await CdnAuthRecoveryPolicy.HandleForbiddenAsync(
            isCdnServer: true,
            isForbidden: true,
            retryingWithReplacementToken: false,
            canRetry: true,
            refreshTokenAsync: _ =>
            {
                refreshCalls++;
                return Task.FromResult(true);
            },
            invalidateRejectedTokenAsync: _ =>
            {
                invalidateCalls++;
                return Task.CompletedTask;
            },
            cancellationToken: CancellationToken.None);

        Assert.Equal(CdnAuthRecoveryAction.RetrySameServer, action);
        Assert.Equal(1, refreshCalls);
        Assert.Equal(0, invalidateCalls);
    }

    [Fact]
    public async Task SecondCdn403_InvalidatesTokenAndDropsServer()
    {
        var refreshCalls = 0;
        var invalidateCalls = 0;

        var action = await CdnAuthRecoveryPolicy.HandleForbiddenAsync(
            isCdnServer: true,
            isForbidden: true,
            retryingWithReplacementToken: true,
            canRetry: true,
            refreshTokenAsync: _ =>
            {
                refreshCalls++;
                return Task.FromResult(true);
            },
            invalidateRejectedTokenAsync: _ =>
            {
                invalidateCalls++;
                return Task.CompletedTask;
            },
            cancellationToken: CancellationToken.None);

        Assert.Equal(CdnAuthRecoveryAction.DropServer, action);
        Assert.Equal(0, refreshCalls);
        Assert.Equal(1, invalidateCalls);
    }

    [Fact]
    public async Task RefreshFails_DropsServer()
    {
        var action = await CdnAuthRecoveryPolicy.HandleForbiddenAsync(
            true,
            true,
            false,
            true,
            _ => Task.FromResult(false),
            _ => Task.CompletedTask,
            CancellationToken.None);

        Assert.Equal(CdnAuthRecoveryAction.DropServer, action);
    }

    [Fact]
    public async Task NoAttemptsRemain_DoesNotRefresh()
    {
        var refreshCalls = 0;

        var action = await CdnAuthRecoveryPolicy.HandleForbiddenAsync(
            true,
            true,
            false,
            false,
            _ =>
            {
                refreshCalls++;
                return Task.FromResult(true);
            },
            _ => Task.CompletedTask,
            CancellationToken.None);

        Assert.Equal(CdnAuthRecoveryAction.DropServer, action);
        Assert.Equal(0, refreshCalls);
    }

    [Theory]
    [InlineData(false, true)]
    [InlineData(true, false)]
    public async Task NonCdnOrNon403_DoesNotAttemptRecovery(
        bool isCdnServer,
        bool isForbidden)
    {
        var refreshCalls = 0;
        var invalidateCalls = 0;

        var action = await CdnAuthRecoveryPolicy.HandleForbiddenAsync(
            isCdnServer,
            isForbidden,
            false,
            true,
            _ =>
            {
                refreshCalls++;
                return Task.FromResult(true);
            },
            _ =>
            {
                invalidateCalls++;
                return Task.CompletedTask;
            },
            CancellationToken.None);

        Assert.Equal(CdnAuthRecoveryAction.None, action);
        Assert.Equal(0, refreshCalls);
        Assert.Equal(0, invalidateCalls);
    }

    [Fact]
    public async Task CancellationDuringRefresh_Propagates()
    {
        var action = CdnAuthRecoveryPolicy.HandleForbiddenAsync(
            true,
            true,
            false,
            true,
            _ => Task.FromException<bool>(
                new OperationCanceledException()),
            _ => Task.CompletedTask,
            CancellationToken.None);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => action);
    }

    [Fact]
    public async Task CancellationDuringInvalidation_Propagates()
    {
        var action = CdnAuthRecoveryPolicy.HandleForbiddenAsync(
            true,
            true,
            true,
            true,
            _ => Task.FromResult(true),
            _ => Task.FromException(
                new OperationCanceledException()),
            CancellationToken.None);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => action);
    }

    [Fact]
    public async Task First403_RefreshesCacheAndMakesReplacementAvailable()
    {
        var now = DateTime.UtcNow;
        var cache = new CdnAuthTokenCache(() => now);
        const string key = "233781:cdn.example.com";

        await cache.RefreshAsync(
            key,
            null,
            _ => Task.FromResult<CdnAuthTokenCache.Entry?>(
                new("old-token", now.AddHours(1))),
            CancellationToken.None);

        var action = await CdnAuthRecoveryPolicy.HandleForbiddenAsync(
            isCdnServer: true,
            isForbidden: true,
            retryingWithReplacementToken: false,
            canRetry: true,
            refreshTokenAsync: ct => cache.RefreshAsync(
                key,
                "old-token",
                _ => Task.FromResult<CdnAuthTokenCache.Entry?>(
                    new("replacement-token", now.AddHours(1))),
                ct),
            invalidateRejectedTokenAsync: ct =>
                cache.InvalidateIfTokenMatchesAsync(key, "old-token", ct),
            cancellationToken: CancellationToken.None);

        Assert.Equal(CdnAuthRecoveryAction.RetrySameServer, action);
        Assert.Equal("replacement-token", cache.GetToken(key));
    }

    [Fact]
    public async Task Second403_InvalidatesCachedReplacementAndDoesNotRefreshAgain()
    {
        var now = DateTime.UtcNow;
        var cache = new CdnAuthTokenCache(() => now);
        const string key = "233781:cdn.example.com";

        await cache.RefreshAsync(
            key,
            null,
            _ => Task.FromResult<CdnAuthTokenCache.Entry?>(
                new("replacement-token", now.AddHours(1))),
            CancellationToken.None);

        var action = await CdnAuthRecoveryPolicy.HandleForbiddenAsync(
            isCdnServer: true,
            isForbidden: true,
            retryingWithReplacementToken: true,
            canRetry: true,
            refreshTokenAsync: _ => throw new InvalidOperationException(
            "A second token refresh must not be attempted."),
            invalidateRejectedTokenAsync: ct =>
                cache.InvalidateIfTokenMatchesAsync(
                    key, "replacement-token", ct),
            cancellationToken: CancellationToken.None);

        Assert.Equal(CdnAuthRecoveryAction.DropServer, action);
        Assert.Null(cache.GetToken(key));
    }

    [Fact]
    public async Task Second403_DoesNotInvalidateANewerCachedToken()
    {
        var now = DateTime.UtcNow;
        var cache = new CdnAuthTokenCache(() => now);
        const string key = "233781:cdn.example.com";

        await cache.RefreshAsync(
            key,
            null,
            _ => Task.FromResult<CdnAuthTokenCache.Entry?>(
                new("replacement-v1", now.AddHours(1))),
            CancellationToken.None);

        // Simulate another worker obtaining a newer token before invalidation.
        await cache.RefreshAsync(
            key,
            "replacement-v1",
            _ => Task.FromResult<CdnAuthTokenCache.Entry?>(
                new("replacement-v2", now.AddHours(1))),
            CancellationToken.None);

        var action = await CdnAuthRecoveryPolicy.HandleForbiddenAsync(
            isCdnServer: true,
            isForbidden: true,
            retryingWithReplacementToken: true,
            canRetry: true,
            refreshTokenAsync: _ => throw new InvalidOperationException(
                "A second token refresh must not be attempted."),
            invalidateRejectedTokenAsync: ct =>
                cache.InvalidateIfTokenMatchesAsync(
                    key, "replacement-v1", ct),
            cancellationToken: CancellationToken.None);

        Assert.Equal(CdnAuthRecoveryAction.DropServer, action);
        Assert.Equal("replacement-v2", cache.GetToken(key));
    }

    [Fact]
    public async Task NoAttemptsRemain_InvalidatesTokenWithoutRefreshing()
    {
        var refreshCalls = 0;
        var invalidateCalls = 0;

        var action = await CdnAuthRecoveryPolicy.HandleForbiddenAsync(
            isCdnServer: true,
            isForbidden: true,
            retryingWithReplacementToken: false,
            canRetry: false,
            refreshTokenAsync: _ =>
            {
                refreshCalls++;
                return Task.FromResult(true);
            },
            invalidateRejectedTokenAsync: _ =>
            {
                invalidateCalls++;
                return Task.CompletedTask;
            },
            cancellationToken: CancellationToken.None);

        Assert.Equal(CdnAuthRecoveryAction.DropServer, action);
        Assert.Equal(0, refreshCalls);
        Assert.Equal(1, invalidateCalls);
    }

    [Fact]
    public async Task FirstCdn403_WithoutCachedToken_ObtainsReplacement()
    {
        var now = DateTime.UtcNow;
        var cache = new CdnAuthTokenCache(() => now);
        const string key = "233781:cdn.example.com";

        Assert.Null(cache.GetToken(key));

        var tokenRequests = 0;
        var invalidationCalls = 0;

        var action = await CdnAuthRecoveryPolicy.HandleForbiddenAsync(
            isCdnServer: true,
            isForbidden: true,
            retryingWithReplacementToken: false,
            canRetry: true,
            refreshTokenAsync: ct => cache.RefreshAsync(
                key,
                rejectedToken: null,
                requestToken: _ =>
                {
                    tokenRequests++;

                    return Task.FromResult<CdnAuthTokenCache.Entry?>(
                        new("replacement-token", now.AddHours(1)));
                },
                cancellationToken: ct),
            invalidateRejectedTokenAsync: ct =>
            {
                invalidationCalls++;

                return cache.InvalidateIfTokenMatchesAsync(
                    key, null, ct);
            },
            cancellationToken: CancellationToken.None);

        Assert.Equal(CdnAuthRecoveryAction.RetrySameServer, action);
        Assert.Equal(1, tokenRequests);
        Assert.Equal(0, invalidationCalls);
        Assert.Equal("replacement-token", cache.GetToken(key));
    }
}