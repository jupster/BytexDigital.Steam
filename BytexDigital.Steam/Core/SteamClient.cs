using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BytexDigital.Steam.ContentDelivery.Exceptions;
using BytexDigital.Steam.Core.Exceptions;
using BytexDigital.Steam.Core.Regional;
using Nito.AsyncEx;
using SteamKit2.Authentication;
using SteamKit2.Discovery;
using SteamKit2.Internal;
using SteamKit = SteamKit2;

namespace BytexDigital.Steam.Core
{
    public sealed class SteamClient : IDisposable
    {
        public enum SteamClientState
        {
            Created,
            Running,
            ShutDown
        }

        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly CancellationToken _token;
        private bool _disposed;
        private readonly AsyncManualResetEvent _clientFaultedEvent = new AsyncManualResetEvent(false);
        private readonly AsyncManualResetEvent _clientReadyEvent = new AsyncManualResetEvent(false);
        private readonly AsyncSemaphore _clientReconnectSemaphore = new AsyncSemaphore(1);
        internal readonly SteamKit.SteamApps _steamAppsHandler;
        private readonly SteamAuthenticator _steamAuthenticator;
        internal readonly SteamKit.SteamContent _steamContentHandler;
        internal readonly SteamKit.SteamUser _steamUserHandler;
        private readonly AsyncManualResetEvent _licensesReceived = new(false);
        private int _logonAttemptsCounter;
        private int _cmAttemptsCounter;

        /// <summary>
        ///     Credentials that this instance will use to log in after connecting.
        /// </summary>
        public SteamCredentials Credentials { get; }

        /// <summary>
        ///     Internal client provided by the SteamKit library.
        /// </summary>
        public SteamKit.SteamClient InternalClient { get; }

        /// <summary>
        ///     Server that the client will be forced to connect to.
        ///     <para>If unavailable, the client will attempt reconnecting until a shutdown is forced.</para>
        /// </summary>
        public ServerRecord? ForcedServer { get; set; }

        public CancellationToken CancellationToken => _token;

        /// <summary>
        ///     Indicates whether the client is ready for any operation.
        /// </summary>
        public bool IsConnected => _clientReadyEvent.IsSet;

        /// <summary>
        ///     Indicates whether the client is faulted. See <see cref="FaultReason" /> for more information.
        /// </summary>
        public bool IsFaulted => FaultException != null;

        /// <summary>
        ///     State of the client. Once in the <see cref="SteamClientState.ShutDown" /> state, it cannot be used anymore.
        /// </summary>
        public SteamClientState State { get; private set; } = SteamClientState.Created;

        public int MaximumLogonAttempts { get; set; } = 1;
        public int MaximumCmServerAttempts { get; set; } = 15;
        public uint ActiveCellId { get; private set; }
        public Exception? FaultException { get; private set; }

        internal SteamKit.CallbackManager CallbackManager { get; set; }
        internal IList<SteamKit.SteamApps.LicenseListCallback.License> Licenses
        { 
            get; 
            set; 
        } = new List<SteamKit.SteamApps.LicenseListCallback.License>();

        public SteamClient(
            SteamCredentials credentials,
            SteamAuthenticator steamAuthenticator,
            Action<SteamKit.ISteamConfigurationBuilder>? steamConfigurationBuilder = null,
            IRegionSpecificServerListProvider? serverListProvider = null)
        {
            // Default values
            credentials ??= SteamCredentials.Anonymous;
            steamConfigurationBuilder ??= builder => { };

            Credentials = credentials;
            _steamAuthenticator = steamAuthenticator ?? throw new SteamAuthenticatorRequiredException();

            // If the caller has provided a region specific server list,
            // make sure we also use it in our SteamConfiguration object.
            if (serverListProvider != null)
            {
                var userPassedBuilder = steamConfigurationBuilder;

                steamConfigurationBuilder = builder =>
                {
                    builder.WithServerListProvider(serverListProvider);

                    // Invoke the user passed config builder afterwards
                    userPassedBuilder.Invoke(builder);
                };
            }

            // SteamKit2 SteamConfiguration object
            var configuration = SteamKit.SteamConfiguration.Create(steamConfigurationBuilder);

            // Region specific server list providers are provided the SteamConfiguration object to make API calls,
            // for example to ISteamDirectory.
            serverListProvider?.SetSteamConfiguration(configuration);

            // Initialize SteamClient with the configuration object
            InternalClient = new SteamKit.SteamClient(configuration);

            _cancellationTokenSource = new CancellationTokenSource();
            _token = _cancellationTokenSource.Token;
            CallbackManager = new SteamKit.CallbackManager(InternalClient);

            _steamUserHandler = InternalClient.GetHandler<SteamKit.SteamUser>()
                ?? throw new InvalidOperationException(
                    "SteamKit failed to provide the SteamUser handler.");

            _steamAppsHandler = InternalClient.GetHandler<SteamKit.SteamApps>()
                ?? throw new InvalidOperationException(
                    "SteamKit failed to provide the SteamApps handler.");

            _steamContentHandler = InternalClient.GetHandler<SteamKit.SteamContent>()
                ?? throw new InvalidOperationException(
                    "SteamKit failed to provide the SteamContent handler.");

            CallbackManager.Subscribe<SteamKit.SteamClient.ConnectedCallback>(OnConnected);
            CallbackManager.Subscribe<SteamKit.SteamClient.DisconnectedCallback>(OnDisconnected);
            CallbackManager.Subscribe<SteamKit.SteamUser.LoggedOnCallback>(OnLoggedOn);
            CallbackManager.Subscribe<SteamKit.SteamUser.LoggedOffCallback>(OnLoggedOff);
            CallbackManager.Subscribe<SteamKit.SteamApps.LicenseListCallback>(OnLicenseList);

            Task.Factory.StartNew(RunCallbacks, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            State = SteamClientState.ShutDown;

            FaultException ??= new SteamClientDisposedException();

            _clientFaultedEvent.Set();
            _cancellationTokenSource.Cancel();

            // The cancellation source is disposed by RunCallbacks once its loop has stopped
            InternalClient.Disconnect();
        }

        /// <summary>
        ///     Event handler that fires upon exception that are produced by user code.
        /// </summary>
        public event EventHandler<Exception>? OnUnhandledException;

        public event Action? InternalClientAttemptingConnect;
        public event Action? InternalClientConnected;
        public event Action? InternalClientDisconnected;
        public event Action? InternalClientLoggedOn;
        public event Action? InternalClientLoggedOff;

        public void Shutdown()
        {
            Dispose();
        }

        /// <summary>
        ///     Asks the underlying client to connect to Steam and perform a login with the given <see cref="Credentials" />.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <exception cref="SteamClientFaultedException">Client is faulted.</exception>
        public async Task ConnectAsync(CancellationToken cancellationToken = default)
        {
            // A client that was shut down or faulted can never be reused, so say that
            // instead of the misleading "already running"
            if (State == SteamClientState.ShutDown) throw new SteamClientFaultedException(FaultException);
            if (State != SteamClientState.Created) throw new SteamClientAlreadyRunningException();
            State = SteamClientState.Running;

            InternalClientAttemptingConnect?.Invoke();

            InternalClient.Connect(ForcedServer);

            var readyTask = _clientReadyEvent.WaitAsync(cancellationToken);
            var faultedTask = _clientFaultedEvent.WaitAsync(cancellationToken);

            var task = await Task.WhenAny(readyTask, faultedTask);

            // WhenAny hands back a cancelled task instead of throwing, so check for it explicitly
            cancellationToken.ThrowIfCancellationRequested();

            if (task == faultedTask) throw new SteamClientFaultedException(FaultException);

            // If we are an anonymous user, the callback about owned licenses will not fire.
            if (!Credentials.IsAnonymous)
            {
                // Wait for all licenses to be received before continuing.
                await _licensesReceived.WaitAsync(cancellationToken);
            }
            else
            {
                // Manually set them to nothing for an anonymous user.
                Licenses = new List<SteamKit.SteamApps.LicenseListCallback.License>();
                _licensesReceived.Set();
            }
        }

        /// <summary>
        ///     Awaits client to be ready for operational use. Throws exception in any other case.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Awaitable.</returns>
        /// <exception cref="SteamClientFaultedException"></exception>
        /// <exception cref="SteamClientNotReadyException"></exception>
        public async Task AwaitReadyAsync(CancellationToken cancellationToken)
        {
            var readyTask = _clientReadyEvent.WaitAsync(cancellationToken);
            var faultedTask = _clientFaultedEvent.WaitAsync(cancellationToken);

            var task = await Task.WhenAny(readyTask, faultedTask);

            // WhenAny hands back a cancelled task instead of throwing, so check for it explicitly
            cancellationToken.ThrowIfCancellationRequested();

            if (task == faultedTask) throw new SteamClientFaultedException(FaultException);

            if (!readyTask.IsCompletedSuccessfully) throw new SteamClientNotReadyException();
        }

        private void AttemptLogin()
        {
            if (!Credentials.IsAnonymous)
            {
                _ = Task.Run(
                    async () =>
                    {
                        _logonAttemptsCounter++;
                        var accessToken = string.Empty;
                        
                        try
                        {
                            accessToken =
                                await _steamAuthenticator.GetAccessTokenAsync(_token);

                            if (string.IsNullOrEmpty(accessToken))
                            {
                                var guardData = await _steamAuthenticator.GetGuardDataAsync(_token);

                                // Begin authenticating via credentials
                                var authSession = await InternalClient.Authentication
                                    .BeginAuthSessionViaCredentialsAsync(
                                        new AuthSessionDetails
                                        {
                                            Username = Credentials.Username,
                                            Password = Credentials.Password,
                                            IsPersistentSession = false,
                                            Authenticator = new SteamAuthenticatorAdapter(_steamAuthenticator),
                                            GuardData = string.IsNullOrEmpty(guardData) ? null : guardData
                                        })
                                    .WaitAsync(_token);

                                var pollResponse = await authSession.PollingWaitForResultAsync(CancellationToken);

                                accessToken = pollResponse.RefreshToken;

                                await _steamAuthenticator.PersistAccessTokenAsync(accessToken,
                                    _token);

                                if (!string.IsNullOrEmpty(pollResponse.NewGuardData))
                                {
                                    await _steamAuthenticator.PersistGuardDataAsync(pollResponse.NewGuardData,
                                        _token);
                                }
                            }
                        }
                        catch (AuthenticationException ex)
                        {
                            SetFaultedAndShutdown(new SteamAuthenticationException(ex.Result));

                            return;
                        }
                        catch (Exception ex)
                        {
                            // Without a token there is nothing to log on with. Report the real cause.
                            OnUnhandledException?.Invoke(this, ex);
                            SetFaultedAndShutdown(ex);

                            return;
                        }
                        
                        // Don't spam unsuccessful logon attempts as this might get us rate limited
                        if (_logonAttemptsCounter > MaximumLogonAttempts)
                        {
                            SetFaultedAndShutdown(new SteamConnectException());

                            return;
                        }

                        _steamUserHandler.LogOn(
                            new SteamKit.SteamUser.LogOnDetails
                            {
                                Username = Credentials.Username,
                                AccessToken = accessToken,
                                LoginID = (uint) Random.Shared.Next(100000000, 999999999)
                            });
                    });
            }
            else
            {
                _steamUserHandler.LogOnAnonymous();
            }
        }

        private void RunCallbacks()
        {
            try
            {
                while (!_token.IsCancellationRequested)
                {
                    CallbackManager.RunWaitAllCallbacks(TimeSpan.FromMilliseconds(10));
                }
            }
            catch (Exception ex)
            {
                // An exception in a callback would otherwise end this loop silently and leave a dead client
                SetFaultedAndShutdown(ex);
            }
            finally
            {
                _cancellationTokenSource.Dispose();
            }
        }

        private void OnLicenseList(SteamKit.SteamApps.LicenseListCallback callback)
        {
            Licenses = new List<SteamKit.SteamApps.LicenseListCallback.License>(callback.LicenseList);

            _licensesReceived.Set();
        }

        private void OnConnected(SteamKit.SteamClient.ConnectedCallback callback)
        {
            InternalClientConnected?.Invoke();

            AttemptLogin();
        }

        private void OnDisconnected(SteamKit.SteamClient.DisconnectedCallback callback)
        {
            InternalClientDisconnected?.Invoke();

            _clientReadyEvent.Reset();

            if (callback.UserInitiated) return;

            if (CancellationToken.IsCancellationRequested) return;

            _ = Task.Run(
                async () =>
                {
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(1), CancellationToken);

                        // Keep this thread hostage until noone is currently performing work that might change how we sign into Steam, e.g.
                        // authentication details such as 2FA code
                        using var lk = await _clientReconnectSemaphore.LockAsync(CancellationToken);

                        CancellationToken.ThrowIfCancellationRequested();

                        InternalClientAttemptingConnect?.Invoke();

                        InternalClient.Connect(ForcedServer);
                    }
                    catch (OperationCanceledException)
                    {
                        // The client was shut down while waiting to reconnect
                    }
                    catch (Exception ex)
                    {
                        OnUnhandledException?.Invoke(this, ex);
                    }
                });
        }

        // The callback manager needs a void method. Everything real happens in HandleLoggedOnAsync,
        // and this wrapper makes sure no exception escapes, because an exception from an async void method crashes the app.
        private async void OnLoggedOn(SteamKit.SteamUser.LoggedOnCallback callback)
        {
            try
            {
                await HandleLoggedOnAsync(callback);
            }
            catch (Exception ex)
            {
                SetFaultedAndShutdown(ex);
            }
        }

        private async Task HandleLoggedOnAsync(SteamKit.SteamUser.LoggedOnCallback callback)
        {
            InternalClientLoggedOn?.Invoke();

            // Locking this semaphore makes the reconnect logic halt until we are done in this method
            using var lk = await _clientReconnectSemaphore.LockAsync();

            if (callback.Result == SteamKit.EResult.OK)
            {
                // Set the cell ID our Steam Client actually connected to
                ActiveCellId = callback.CellID;

                _logonAttemptsCounter = 0;
                _cmAttemptsCounter    = 0;

                _clientReadyEvent.Set();
                return;
            }

            if (callback.Result == SteamKit.EResult.AccountLogonDenied ||
                callback.Result == SteamKit.EResult.AccountLoginDeniedNeedTwoFactor)
            {
                // By returning, we are releasing the aquired semaphore. This will make the disconnected
                // callback continue it's execution and attempt a reconnect if our client hasn't been marked
                // as cancelled/closed.
                return;
            }

            if (callback.Result == SteamKit.EResult.TryAnotherCM)
            {
                // If we get this error code, we should try another CM server to connect to. This one might just be
                // down for maintenance. Keep track of how many times we attempted to connect to different CM
                // servers to set a limit though.
                _cmAttemptsCounter += 1;

                if (_cmAttemptsCounter < MaximumCmServerAttempts)
                {
                    // Returning here will result in a new connect attempt. It's important we DONT create a new
                    // SteamKit.SteamClient as it already keeps track of which CM servers were attempted.
                    return;
                }

                SetFaultedAndShutdown(new SteamNoCmServerFoundException());
                return;
            }

            // Don't spam unsuccessful logon attempts as this might get us rate limited
            SetFaultedAndShutdown(new SteamAuthenticationException(callback.Result));
        }

        private void SetFaultedAndShutdown(Exception exception)
        {
            FaultException = exception;
            
            Shutdown();
        }

        private void OnLoggedOff(SteamKit.SteamUser.LoggedOffCallback callback)
        {
            InternalClientLoggedOff?.Invoke();

            _clientReadyEvent.Reset();
        }
    }
}