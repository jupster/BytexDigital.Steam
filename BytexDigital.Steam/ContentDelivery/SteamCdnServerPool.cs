using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using BytexDigital.Steam.ContentDelivery.Exceptions;
using BytexDigital.Steam.Core.Structs;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx;
using SteamKit2;
using SteamKit2.CDN;

namespace BytexDigital.Steam.ContentDelivery
{
    public class SteamCdnServerPool : IDisposable
    {
        private const int MinimumPoolSize = 10;
        private const int MaxThrottleDelaySeconds = 60;
        private static readonly TimeSpan PopulateCheckInterval = TimeSpan.FromSeconds(5);

        private readonly ConcurrentStack<Server> _activeServerEndpoints = new();
        private readonly AppId _appId;
        private readonly BlockingCollection<Server> _availableServerEndpoints = new();
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly CancellationToken _token;
        private readonly AsyncManualResetEvent _populatedEvent = new(false);
        private readonly AsyncAutoResetEvent _populatePoolEvent = new(true);
        private readonly SteamContentClient _steamContentClient;

        public ILogger Logger { get; set; }
        public Client CdnClient { get; }
        public Server DesignatedProxyServer { get; private set; }
        public bool IsExhausted { get; private set; }

        public SteamCdnServerPool(
            SteamContentClient steamContentClient,
            AppId appId,
            CancellationToken cancellationToken = default)
        {
            _steamContentClient = steamContentClient;
            _appId = appId;
            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _token = _cancellationTokenSource.Token;

            CdnClient = new Client(_steamContentClient.SteamClient.InternalClient);

            // MonitorAsync handles its own errors, so it is safe to leave running on its own
            _ = Task.Run(MonitorAsync);
        }

        public void Dispose()
        {
            Close();
            _cancellationTokenSource.Dispose();
        }

        public void Close()
        {
            _cancellationTokenSource.Cancel();
        }

        public Server GetServer(CancellationToken cancellationToken = default)
        {
            // Stops waiting when the caller cancels OR when the pool is closed or gives up
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _token);
            Server server;

            do
            {
                if (!_activeServerEndpoints.TryPop(out server))
                {
                    if (_availableServerEndpoints.Count < MinimumPoolSize && !_populatedEvent.IsSet)
                    {
                        Logger?.LogTrace("GetServer: Set populatePoolEvent");
                        _populatePoolEvent.Set();
                    }

                    try
                    {
                        server = _availableServerEndpoints.Take(linked.Token);
                    }
                    catch (OperationCanceledException) when (IsExhausted && !cancellationToken.IsCancellationRequested)
                    {
                        throw new SteamNoContentServerFoundException(_appId);
                    }
                }
            } while (server == null);

            return server;
        }

        public void ReturnServer(Server server, bool isFaulty)
        {
            if (isFaulty)
            {
                return;
            }

            _activeServerEndpoints.Push(server);
        }

        private async Task WaitForPopulateRequestAsync()
        {
            // Wake up when someone asks for more servers, or after a few seconds to check anyway
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(_token);
            timeout.CancelAfter(PopulateCheckInterval);

            try
            {
                await _populatePoolEvent.WaitAsync(timeout.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!_token.IsCancellationRequested)
            {
                // Only the timer fired, which is expected
            }
        }

        private async Task MonitorAsync()
        {
            try
            {
                while (!_token.IsCancellationRequested)
                {
                    Logger?.LogTrace("MonitorAsync: Waiting before checking if pool needs population");
                    await WaitForPopulateRequestAsync().ConfigureAwait(false);

                    Logger?.LogTrace("MonitorAsync: Checking");
                    if (_availableServerEndpoints.Count < MinimumPoolSize)
                    {
                        Logger?.LogTrace("MonitorAsync: Pool will be populated");
                        _populatedEvent.Reset();

                        var servers = await GetServerListAsync().ConfigureAwait(false);

                        if (servers.Count == 0)
                        {
                            IsExhausted = true;
                            _populatedEvent.Set();
                            _cancellationTokenSource.Cancel();

                            return;
                        }

                        DesignatedProxyServer = servers.FirstOrDefault(x => x.UseAsProxy);

                        var sortedServers = servers
                            // Filter out servers that aren't relevant to us or our appid
                            .Where(
                                server =>
                                {
                                    var isContentServer = server.Type == "SteamCache" || server.Type == "CDN";
                                    var allowsAppId = server.AllowedAppIds.Length == 0 ||
                                                      server.AllowedAppIds.Contains(_appId);

                                    return isContentServer && allowsAppId;
                                })
                            .OrderBy(x => x.WeightedLoad);

                        foreach (var server in sortedServers)
                        {
                            _availableServerEndpoints.Add(server);
                        }
                    }

                    Logger?.LogTrace("MonitorAsync: Notifying that pool is populated");
                    _populatedEvent.Set();
                }
            }
            catch (OperationCanceledException)
            {
                // The pool was closed
            }
            catch (Exception ex)
            {
                // If the monitor dies nobody will ever add servers, so make waiting callers give up instead of hanging
                Logger?.LogError(ex, "CDN server pool monitor stopped unexpectedly");
                IsExhausted = true;
                _populatedEvent.Set();

                if (!_token.IsCancellationRequested)
                {
                    _cancellationTokenSource.Cancel();
                }
            }
        }

        private async Task<IReadOnlyCollection<Server>> GetServerListAsync()
        {
            var throttleDelay = 0;

            while (true)
            {
                _token.ThrowIfCancellationRequested();

                await Task.Delay(TimeSpan.FromSeconds(1), _token).ConfigureAwait(false);

                try
                {
                    var cdnServers = await ContentServerDirectoryService.LoadAsync(
                        _steamContentClient.SteamClient.InternalClient.Configuration,
                        _steamContentClient.SteamClient.ActiveCellId,
                        _token).ConfigureAwait(false);

                    if (cdnServers == null)
                    {
                        throw new InvalidOperationException(
                            "ContentServerDirectoryService did not return a list of servers.");
                    }

                    return cdnServers;
                }
                catch (OperationCanceledException) when (_token.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    Logger?.LogWarning(ex, "Could not load the CDN server list, trying again");

                    if (ex is SteamKitWebRequestException { StatusCode: HttpStatusCode.TooManyRequests })
                    {
                        throttleDelay = Math.Min(throttleDelay + 5, MaxThrottleDelaySeconds);

                        await Task.Delay(TimeSpan.FromSeconds(throttleDelay), _token).ConfigureAwait(false);
                    }
                }
            }
        }
    }
}