using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Centrifugal.Centrifuge.Transports;
using Centrifugal.Centrifuge.Protocol;
using Google.Protobuf;
using Microsoft.Extensions.Logging;

namespace Centrifugal.Centrifuge
{
    /// <summary>
    /// A server-side subscription's registry entry. Immutable: the registry replaces entries. A delivery
    /// advances an entry within its <see cref="Base"/> (see AdvanceServerSubscription); a new base
    /// replaces it whatever deliveries of the old one advanced.
    /// </summary>
    internal sealed class ServerSubscription
    {
        public ServerSubscription(ulong offset, string epoch, bool recoverable)
            : this(offset, epoch, recoverable, null)
        {
        }

        private ServerSubscription(ulong offset, string epoch, bool recoverable, ServerSubscription? @base)
        {
            Offset = offset;
            Epoch = epoch;
            Recoverable = recoverable;
            Base = @base ?? this;
        }

        public ulong Offset { get; }
        public string Epoch { get; }
        public bool Recoverable { get; }

        /// <summary>The entry that started this base.</summary>
        public ServerSubscription Base { get; }

        /// <summary>The entry past a publication within this base, or null when it isn't newer.</summary>
        public ServerSubscription? Past(ulong offset, string epoch) =>
            new CentrifugeStreamPosition(Offset, Epoch).Past(offset, epoch) is { } next
                ? new ServerSubscription(next.Offset, next.Epoch, Recoverable, Base)
                : null;
    }

    /// <summary>
    /// Centrifuge client for real-time messaging with Centrifugo server.
    /// </summary>
    public class CentrifugeClient : IDisposable, IAsyncDisposable
    {
        private readonly string? _endpoint;
        /// <summary>The transport the single <see cref="_endpoint"/> selects by its scheme.</summary>
        private readonly CentrifugeTransportType _endpointTransport;
        private readonly List<CentrifugeTransportEndpoint>? _transportEndpoints;
        private readonly CentrifugeClientOptions _options;
        /// <summary>The connection token, data and headers: taken from the options at construction and
        /// changed by the setters and token refreshes — never written back into the options, which the
        /// app may share between clients.</summary>
        private string? _token;
        private ReadOnlyMemory<byte> _data;
        private Dictionary<string, string>? _headers;
        private readonly ConcurrentDictionary<string, CentrifugeSubscription> _subscriptions = new();
        // Channel compaction: numeric channel ID → subscription, used to route pushes
        // that carry an ID instead of the string channel name. IDs are scoped to a
        // server session — the registry is dropped on transport teardown and each
        // subscription re-registers from its subscribe reply.
        private readonly ConcurrentDictionary<long, CentrifugeSubscription> _subscriptionsByPushId = new();
        private readonly ConcurrentDictionary<string, ServerSubscription> _serverSubscriptions = new();
        /// <summary>The server-side publication whose handler runs, in its base; Seq names the owning delivery —
        /// receive loops of consecutive sessions may overlap.</summary>
        private (long Seq, string Channel, ServerSubscription Base, ulong Offset, string Epoch)? _deliveringServerPublication;
        private long _serverDeliverySeq;
        /// <summary>
        /// Server-side channels whose Connected session ended and whose ServerSubscribing the app
        /// hasn't received yet (under _stateChangeLock): the teardown owes it, and so does whatever
        /// comes first on the channel — a nested teardown or the next ServerSubscribed/ServerUnsubscribed.
        /// </summary>
        private readonly HashSet<string> _serverSubscribingOwed = new();
        private readonly ConcurrentDictionary<uint, TaskCompletionSource<Reply>> _pendingCalls = new();
        private readonly object _stateChangeLock = new object();
        private readonly ReadyPromises _readyPromises = new();
        private readonly List<(Command Command, int Size, TaskCompletionSource<bool> Written)> _commandBatch = new();
        /// <summary>
        /// The deadline of the send being written, taken off <see cref="_commandBatch"/>: the teardown of
        /// its session takes it over and cancels it (CancelWrite) instead of leaving the callers to a stuck
        /// write. The outcome belongs to the write — one that completed stays written (see
        /// FlushCommandBatchAsync). Whoever takes it off this field disposes it, so its token is read with
        /// the batch, under _commandBatchLock.
        /// </summary>
        private CancellationTokenSource? _writingDeadline;
        private readonly object _commandBatchLock = new object();
        /// <summary>
        /// Serializes the flushes of one transport session. Replaced with the session in
        /// <see cref="DetachTransportLocked"/>, so a send stuck on a torn-down transport doesn't hold up the next one.
        /// </summary>
        private SemaphoreSlim _flushLock = new SemaphoreSlim(1, 1);

        private ITransport? _transport;
        private volatile CentrifugeClientState _state = CentrifugeClientState.Disconnected;
        private int _epoch;
        private int _commandId;
        private int _reconnectAttempts;
        /// <summary>An emulation session's connect reply resets _reconnectAttempts only once the session
        /// proves steady — at a server ping or the next command reply (as centrifuge-js): a stream the
        /// network drops right after each connect would otherwise reconnect without backoff.</summary>
        private bool _reconnectAttemptsResetPending;
        private CancellationTokenSource? _reconnectCts;

        // Internal property to allow Subscription to access timeout
        internal TimeSpan Timeout => _options.Timeout;
        private Timer? _pingTimer;
        private Timer? _refreshTimer;
        private uint _serverPingInterval;
        private bool _sendPong;
        private string _session = string.Empty;
        private string _node = string.Empty;
        /// <summary>The pending call of the connect an emulation transport sends with its open request; set
        /// and cleared with _transport.</summary>
        private (uint Id, TaskCompletionSource<Reply> Call)? _pendingConnectCall;
        /// <summary>The emulation endpoint of the current emulation transport, derived from the endpoint it
        /// was created for; set and cleared with _transport.</summary>
        private string? _emulationEndpoint;
        private volatile int _disposed;
        private int _refreshAttempts;
        private bool _refreshRequired;
        private int _currentTransportIndex;
        /// <summary>A transport of this connect proved to reach the server — WebSocket by opening, emulation
        /// by a connect reply (error included) or a server close (code ≥ 3000), as centrifuge-js: until then
        /// a failed attempt moves a fallback client to the next endpoint (AdvanceUnprovenTransportLocked).</summary>
        private bool _transportWasOpen;
        private Timer? _commandBatchTimer;
        /// <summary>A flush is scheduled — by _commandBatchTimer or, over MaxCommandBatchSize, at once — and
        /// hasn't taken the queue yet: further commands don't schedule another.</summary>
        private bool _commandBatchPending;
        private int _commandBatchSize;
#if NET6_0_OR_GREATER
        private static volatile Microsoft.JSInterop.IJSRuntime? _globalJSRuntime;
        private readonly Microsoft.JSInterop.IJSRuntime? _jsRuntime;
#endif
        private readonly ILogger? _logger;

        /// <summary>
        /// Maximum size of a command batch in bytes (15KB).
        /// When batch exceeds this size, it will be flushed immediately.
        /// </summary>
        private const int MaxCommandBatchSize = 15 * 1024;

        /// <summary>
        /// Command batching delay in milliseconds.
        /// Commands sent within this window will be automatically batched together.
        /// </summary>
        private const int CommandBatchDelayMs = 1;

        /// <summary>
        /// Gets the current client state.
        /// </summary>
        public CentrifugeClientState State => _state;

        /// <summary>
        /// Event raised when client state changes.
        /// </summary>
        /// <remarks>
        /// <para><b>Best Practice:</b> Keep event handlers fast to avoid blocking the real-time message processing pipeline.</para>
        /// <para><b>Async Operations:</b> Use <c>async void</c> with <c>await</c> for I/O operations.</para>
        /// <para><b>CRITICAL:</b> Never block on SDK async methods (e.g., <c>PublishAsync().Wait()</c>) - this will cause deadlock!</para>
        /// <para>Blocking on non-SDK operations (database calls, file I/O, etc.) is safe but not recommended for performance.</para>
        /// </remarks>
        /// <example>
        /// <code>
        /// // BEST ✓ - Async I/O operations
        /// client.StateChanged += async (sender, e) => {
        ///     await LogToFileAsync(e.NewState);
        /// };
        ///
        /// // OK - Synchronous operations on other libraries (won't deadlock, but may be slow)
        /// client.StateChanged += (sender, e) => {
        ///     File.WriteAllText("state.txt", e.NewState.ToString());  // Safe but blocks thread
        /// };
        ///
        /// // DEADLOCK ✗ - Never block on SDK methods!
        /// client.StateChanged += (sender, e) => {
        ///     client.RpcAsync("method", data).Wait();  // DEADLOCK!
        /// };
        /// </code>
        /// </example>
        public event EventHandler<CentrifugeStateEventArgs>? StateChanged;

        /// <summary>
        /// Event raised when client is connecting or reconnecting.
        /// </summary>
        /// <remarks>
        /// <para><b>Best Practice:</b> Keep handlers fast. Use <c>async void</c> with <c>await</c> for I/O operations.</para>
        /// <para><b>CRITICAL:</b> Never block on SDK async methods - this will cause deadlock!</para>
        /// </remarks>
        public event EventHandler<CentrifugeConnectingEventArgs>? Connecting;

        /// <summary>
        /// Event raised when client successfully connects to the server.
        /// This is a good place to set up subscriptions or send initial data.
        /// </summary>
        /// <remarks>
        /// <para><b>Best Practice:</b> Keep handlers fast to avoid delaying message processing.</para>
        /// <para><b>Async SDK Methods:</b> You can safely call <c>PublishAsync</c>, <c>RpcAsync</c>, etc. using <c>await</c> in an <c>async void</c> handler.</para>
        /// <para><b>CRITICAL:</b> Never use <c>.Wait()</c>, <c>.Result</c>, or <c>.GetAwaiter().GetResult()</c> on SDK async methods - this will cause deadlock!</para>
        /// <para>Handlers are invoked on the transport receive thread, so blocking on non-SDK operations (like database calls) won't deadlock but delays processing of incoming messages.</para>
        /// </remarks>
        /// <example>
        /// <code>
        /// // BEST ✓ - Async SDK operations with await; the subscription is created once, before
        /// // Connect: the client resubscribes it on every reconnect
        /// var sub = client.NewSubscription("chat");
        /// sub.Subscribe();
        /// client.Connected += async (sender, e) => {
        ///     Console.WriteLine($"Connected! Client ID: {e.ClientId}");
        ///     await sub.ReadyAsync();
        ///     await sub.PublishAsync(Encoding.UTF8.GetBytes("Hello!"));  // Safe!
        /// };
        ///
        /// // OK - Synchronous non-SDK work (safe but blocks the receive thread)
        /// client.Connected += (sender, e) => {
        ///     database.UpdateConnectionStatus(e.ClientId);  // Won't deadlock
        /// };
        ///
        /// // DEADLOCK ✗ - Never block on SDK async methods!
        /// client.Connected += (sender, e) => {
        ///     sub.PublishAsync(data).Wait();  // DEADLOCK! Use 'async/await' instead
        /// };
        /// </code>
        /// </example>
        public event EventHandler<CentrifugeConnectedEventArgs>? Connected;

        /// <summary>
        /// Event raised when client is disconnected from the server.
        /// </summary>
        /// <remarks>
        /// <para><b>Best Practice:</b> Keep handlers fast. Use <c>async void</c> with <c>await</c> for I/O operations.</para>
        /// <para><b>Note:</b> Don't block on SDK async methods - will cause deadlock. Blocking on non-SDK operations is safe but impacts performance.</para>
        /// </remarks>
        public event EventHandler<CentrifugeDisconnectedEventArgs>? Disconnected;

        /// <summary>
        /// Event raised when an error occurs. Mostly for logging purposes.
        /// </summary>
        /// <remarks>
        /// <para><b>Best Practice:</b> Keep handlers fast. Use <c>async void</c> with <c>await</c> for I/O operations.</para>
        /// <para><b>Exception Handling:</b> Exceptions in <c>async void</c> handlers cannot be caught by the SDK.
        /// Always use try-catch in your handlers to prevent application crashes.</para>
        /// </remarks>
        /// <example>
        /// <code>
        /// client.Error += async (sender, e) => {
        ///     try {
        ///         await LogErrorAsync(e.Type, e.Message);
        ///     }
        ///     catch (Exception ex) {
        ///         Console.WriteLine($"Logging failed: {ex.Message}");
        ///     }
        /// };
        /// </code>
        /// </example>
        public event EventHandler<CentrifugeErrorEventArgs>? Error;

        /// <summary>
        /// Event raised when a message is received from server.
        /// </summary>
        /// <remarks>
        /// Keep handlers fast. Don't block on SDK async methods (will deadlock). Use <c>async void</c> with <c>await</c> for I/O.
        /// </remarks>
        public event EventHandler<CentrifugeMessageEventArgs>? Message;

        /// <summary>
        /// Event raised for server-side subscription publications.
        /// </summary>
        /// <remarks>
        /// Keep handlers fast. Don't block on SDK async methods (will deadlock). Use <c>async void</c> with <c>await</c> for I/O.
        /// </remarks>
        public event EventHandler<CentrifugePublicationEventArgs>? Publication;

        /// <summary>
        /// Event raised for server-side subscription join events.
        /// </summary>
        /// <remarks>
        /// Keep handlers fast. Don't block on SDK async methods (will deadlock). Use <c>async void</c> with <c>await</c> for I/O.
        /// </remarks>
        public event EventHandler<CentrifugeJoinEventArgs>? Join;

        /// <summary>
        /// Event raised for server-side subscription leave events.
        /// </summary>
        /// <remarks>
        /// Keep handlers fast. Don't block on SDK async methods (will deadlock). Use <c>async void</c> with <c>await</c> for I/O.
        /// </remarks>
        public event EventHandler<CentrifugeLeaveEventArgs>? Leave;

        /// <summary>
        /// Event raised when server-side subscription is subscribing: once for a channel
        /// the server announces for the first time, and again for every channel in the
        /// registry whenever a connected session is lost (transport closed, no ping, or
        /// an explicit <see cref="Disconnect"/>).
        /// </summary>
        /// <remarks>
        /// Keep handlers fast. Don't block on SDK async methods (will deadlock). Use <c>async void</c> with <c>await</c> for I/O.
        /// </remarks>
        public event EventHandler<CentrifugeServerSubscribingEventArgs>? ServerSubscribing;

        /// <summary>
        /// Event raised when server-side subscription is subscribed.
        /// </summary>
        /// <remarks>
        /// Keep handlers fast. Don't block on SDK async methods (will deadlock). Use <c>async void</c> with <c>await</c> for I/O.
        /// </remarks>
        public event EventHandler<CentrifugeServerSubscribedEventArgs>? ServerSubscribed;

        /// <summary>
        /// Event raised when server-side subscription is unsubscribed.
        /// </summary>
        /// <remarks>
        /// Keep handlers fast. Don't block on SDK async methods (will deadlock). Use <c>async void</c> with <c>await</c> for I/O.
        /// </remarks>
        public event EventHandler<CentrifugeServerUnsubscribedEventArgs>? ServerUnsubscribed;

#if NET6_0_OR_GREATER
        /// <summary>
        /// Initializes browser interop for Blazor WebAssembly support.
        /// Call this once at application startup (e.g., in Program.cs) to enable browser-native transports without passing IJSRuntime to every client constructor.
        /// </summary>
        /// <param name="jsRuntime">The IJSRuntime instance to use for all clients.</param>
        public static void InitializeBrowserInterop(Microsoft.JSInterop.IJSRuntime jsRuntime)
        {
            _globalJSRuntime = jsRuntime ?? throw new ArgumentNullException(nameof(jsRuntime));
        }
#endif

        /// <summary>
        /// Initializes a new instance of the <see cref="CentrifugeClient"/> class. The endpoint's scheme
        /// selects the transport: ws/wss — WebSocket, http/https — HTTP streaming.
        /// </summary>
        /// <param name="endpoint">WebSocket or HTTP streaming endpoint URL.</param>
        /// <param name="options">Client options.</param>
        /// <exception cref="ArgumentException">The endpoint isn't an absolute ws, wss, http or https URL.</exception>
        public CentrifugeClient(string endpoint, CentrifugeClientOptions? options = null)
            : this(options)
        {
            if (endpoint == null)
            {
                throw new ArgumentNullException(nameof(endpoint));
            }

            _endpoint = endpoint;
            _endpointTransport = TransportOf(endpoint, nameof(endpoint));
            if (_endpointTransport == CentrifugeTransportType.HttpStream) ValidateEmulationEndpoint();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CentrifugeClient"/> class with multi-transport fallback.
        /// The endpoints are copied: later changes to them don't reach the client. In the browser an endpoint
        /// goes to fetch/WebSocket as given (a relative one resolves against the page).
        /// </summary>
        /// <param name="transportEndpoints">Array of transport endpoints to try in order.</param>
        /// <param name="options">Client options.</param>
        /// <exception cref="ArgumentException">The list is empty, or, outside the browser, an endpoint isn't an
        /// absolute URL whose scheme is of its transport.</exception>
        public CentrifugeClient(CentrifugeTransportEndpoint[] transportEndpoints, CentrifugeClientOptions? options = null)
            : this(options)
        {
            if (transportEndpoints == null || transportEndpoints.Length == 0)
            {
                throw new ArgumentException("Transport endpoints cannot be null or empty", nameof(transportEndpoints));
            }

            _transportEndpoints = new List<CentrifugeTransportEndpoint>(transportEndpoints.Length);
            foreach (var endpoint in transportEndpoints)
            {
                if (endpoint == null || (!IsBrowser && TransportOf(endpoint.Endpoint, nameof(transportEndpoints)) != endpoint.Transport))
                {
                    throw new ArgumentException($"Endpoint '{endpoint?.Endpoint}' doesn't match its transport", nameof(transportEndpoints));
                }
                _transportEndpoints.Add(new CentrifugeTransportEndpoint(endpoint.Transport, endpoint.Endpoint));
            }
            if (_transportEndpoints.Exists(endpoint => endpoint.Transport == CentrifugeTransportType.HttpStream))
                ValidateEmulationEndpoint();
        }

        /// <summary>
        /// The transport an endpoint URL's scheme selects: ws/wss — WebSocket, http/https — HTTP streaming.
        /// An endpoint that isn't such an absolute URL is rejected: no transport could ever connect to it.
        /// </summary>
        private static CentrifugeTransportType TransportOf(string? endpoint, string paramName) =>
            Uri.TryCreate(endpoint, UriKind.Absolute, out var uri)
                ? uri.Scheme.ToLowerInvariant() switch
                {
                    "ws" or "wss" => CentrifugeTransportType.WebSocket,
                    "http" or "https" => CentrifugeTransportType.HttpStream,
                    _ => throw new ArgumentException("Endpoint must start with ws://, wss://, http://, or https://", paramName)
                }
                : throw new ArgumentException($"Endpoint is not an absolute URL: '{endpoint}'", paramName);

        /// <summary>
        /// Applies a copy of the options. A browser environment without IJSRuntime can't create any
        /// transport: a configuration error at construction rather than endless connect attempts.
        /// </summary>
        private CentrifugeClient(CentrifugeClientOptions? options)
        {
            _options = options?.Clone() ?? new CentrifugeClientOptions();
            _options.Validate();
            _token = _options.Token;
            _data = _options.Data.IsEmpty ? default : _options.Data.ToArray();
            _headers = _options.Headers != null ? new Dictionary<string, string>(_options.Headers) : null;
            _logger = GuardedLogger.Wrap(_options.Logger);
#if NET6_0_OR_GREATER
            _jsRuntime = _options.JSRuntime ?? _globalJSRuntime;
            if (_jsRuntime == null && OperatingSystem.IsBrowser())
            {
                throw new CentrifugeConfigurationException(
                    "Running in browser environment but IJSRuntime not provided. " +
                    "Either call CentrifugeClient.InitializeBrowserInterop(jsRuntime) at application startup, " +
                    "or pass IJSRuntime via CentrifugeClientOptions.JSRuntime.");
            }
#endif
        }

        /// <summary>Transports run through JS interop: endpoints go to fetch/WebSocket as given.</summary>
        private bool IsBrowser
        {
            get
            {
#if NET6_0_OR_GREATER
                return _jsRuntime != null;
#else
                return false;
#endif
            }
        }

        /// <summary>
        /// An HTTP stream sends its commands to the emulation endpoint: one that isn't an http/https URL
        /// would fail every command after connect — a configuration error at construction rather than
        /// endless reconnects. In the browser fetch also resolves one relative to the page.
        /// </summary>
        private void ValidateEmulationEndpoint()
        {
            var endpoint = _options.EmulationEndpoint;
            if (string.IsNullOrEmpty(endpoint) || IsHttpUrl(endpoint)) return;
            if (IsBrowser && Uri.TryCreate(endpoint, UriKind.Relative, out _)) return;

            throw new CentrifugeConfigurationException(IsBrowser
                ? "EmulationEndpoint must be an http:// or https:// URL or relative to the page"
                : "EmulationEndpoint must be an absolute http:// or https:// URL");
        }

        /// <summary>An absolute http/https URL. The scheme decides, not UriKind.Absolute: outside Windows a
        /// rooted path parses as an absolute file: URI.</summary>
        private static bool IsHttpUrl(string? value) =>
            Uri.TryCreate(value, UriKind.Absolute, out var uri) && uri.Scheme is "http" or "https";

        /// <summary>
        /// Connects to the Centrifugo server. This method returns immediately and starts the connection process in the background.
        /// Use ReadyAsync() to wait for the connection to be established, or use the Connected event.
        /// </summary>
        public void Connect() => StartConnecting();

        private void ThrowIfDisposed()
        {
            if (System.Threading.Interlocked.CompareExchange(ref _disposed, 0, 0) != 0)
                throw new ObjectDisposedException(nameof(CentrifugeClient));
        }

        /// <summary>
        /// Disconnects from the Centrifugo server. This method returns immediately and starts the disconnection process in the background.
        /// </summary>
        public void Disconnect()
        {
            _ = SetDisconnectedAsync(CentrifugeDisconnectedCodes.DisconnectCalled, "disconnect called");
        }

        /// <summary>
        /// Returns a Task that completes when the client is connected.
        /// If the client is already connected, the Task completes immediately.
        /// If the client is disconnected, the Task is rejected.
        /// </summary>
        /// <param name="timeout">Optional timeout.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task that completes when connected.</returns>
        /// <exception cref="ArgumentOutOfRangeException">The timeout is negative (other than infinite) or too large for a timer.</exception>
        public Task ReadyAsync(TimeSpan? timeout = null, CancellationToken cancellationToken = default)
        {
            Utilities.ValidateWaitTimeout(timeout, nameof(timeout));
            if (System.Threading.Interlocked.CompareExchange(ref _disposed, 0, 0) != 0)
                return Task.FromException(new ObjectDisposedException(nameof(CentrifugeClient)));

            lock (_stateChangeLock)
            {
                switch (_state)
                {
                    case CentrifugeClientState.Disconnected:
                        return Task.FromException(new CentrifugeException(CentrifugeErrorCodes.ClientDisconnected, "client disconnected"));

                    case CentrifugeClientState.Connected:
                        return Task.CompletedTask;
                }

                return _readyPromises.Add(timeout, cancellationToken);
            }
        }


        /// <summary>
        /// Creates a new subscription to a channel. It is registered before the disposal check, so a
        /// concurrent Dispose() either disposes it or makes this throw.
        /// </summary>
        /// <param name="channel">Channel name.</param>
        /// <param name="options">Subscription options.</param>
        /// <returns>The subscription instance.</returns>
        public CentrifugeSubscription NewSubscription(string channel, CentrifugeSubscriptionOptions? options = null)
        {
            if (string.IsNullOrWhiteSpace(channel))
            {
                throw new ArgumentException("Channel cannot be null or empty", nameof(channel));
            }

            var subscription = new CentrifugeSubscription(this, channel, options);
            if (!_subscriptions.TryAdd(channel, subscription))
            {
                throw new CentrifugeDuplicateSubscriptionException(channel);
            }
            if (System.Threading.Interlocked.CompareExchange(ref _disposed, 0, 0) != 0)
            {
                Unregister(subscription);
                ThrowIfDisposed();
            }
            return subscription;
        }

        /// <summary>
        /// Gets an existing subscription.
        /// </summary>
        /// <param name="channel">Channel name.</param>
        /// <returns>The subscription instance, or null if not found.</returns>
        public CentrifugeSubscription? GetSubscription(string channel)
        {
            _subscriptions.TryGetValue(channel, out var subscription);
            return subscription;
        }

        /// <summary>
        /// Removes a subscription (see <see cref="CentrifugeSubscription.Dispose"/>) — this instance
        /// only: a newer one of the same channel stays.
        /// </summary>
        /// <param name="subscription">The subscription to remove.</param>
        public void RemoveSubscription(CentrifugeSubscription subscription)
        {
            if (subscription == null) throw new ArgumentNullException(nameof(subscription));
            subscription.Dispose();
        }

        /// <summary>Removes <paramref name="subscription"/> from the registry if it is still the one of its channel.</summary>
        internal void Unregister(CentrifugeSubscription subscription) =>
            ((ICollection<KeyValuePair<string, CentrifugeSubscription>>)_subscriptions)
                .Remove(new KeyValuePair<string, CentrifugeSubscription>(subscription.Channel, subscription));


        /// <summary>
        /// Gets all subscriptions.
        /// </summary>
        public IReadOnlyDictionary<string, CentrifugeSubscription> Subscriptions => _subscriptions;

        /// <summary>
        /// Sets the connection token. Can be used to update token or reset to empty. Without GetToken it
        /// replaces an expired one: the next connect sends it instead of failing Unauthorized; with GetToken
        /// an expired token is still refreshed through it (as centrifuge-js).
        /// </summary>
        /// <param name="token">New connection token (JWT).</param>
        public void SetToken(string? token)
        {
            lock (_stateChangeLock)
            {
                _token = token;
                if (_options.GetToken == null) _refreshRequired = false;
            }
        }

        /// <summary>
        /// Sets the connection data. This will be used for all subsequent connection attempts.
        /// The data is copied internally to prevent external modifications.
        /// </summary>
        /// <param name="data">New connection data.</param>
        public void SetData(ReadOnlyMemory<byte> data)
        {
            lock (_stateChangeLock)
            {
                _data = data.IsEmpty ? default : data.ToArray();
            }
        }

        /// <summary>
        /// Sets the connection headers (emulated headers sent with first protocol message).
        /// Requires Centrifugo v6+.
        /// The headers dictionary is copied internally to prevent external modifications.
        /// </summary>
        /// <param name="headers">Headers to set; values must not be null.</param>
        public void SetHeaders(Dictionary<string, string>? headers)
        {
            if (headers != null && headers.ContainsValue(null!))
                throw new ArgumentException("Header values must not be null", nameof(headers));
            lock (_stateChangeLock)
            {
                _headers = headers != null ? new Dictionary<string, string>(headers) : null;
            }
        }

        /// <summary>
        /// Sends an RPC call to the server.
        /// Automatically waits for the client to be connected before sending.
        /// </summary>
        /// <param name="method">RPC method name.</param>
        /// <param name="data">Request data.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>RPC result.</returns>
        public async Task<CentrifugeRpcResult> RpcAsync(string method, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            // Wait for client to be ready
            await ReadyAsync(_options.Timeout, cancellationToken).ConfigureAwait(false);

            var cmd = new Command
            {
                Id = NextCommandId(),
                Rpc = new RPCRequest
                {
                    Method = method,
                    Data = ByteString.CopyFrom(data.Span)
                }
            };

            var reply = await SendCommandAsync(cmd, cancellationToken).ConfigureAwait(false);

            if (reply.Error != null)
            {
                throw CentrifugeException.FromReply(reply.Error);
            }

            return new CentrifugeRpcResult(reply.Rpc?.Data.ToByteArray() ?? Array.Empty<byte>());
        }

        /// <summary>
        /// Sends an asynchronous message to the server (no response expected).
        /// Automatically waits for the client to be connected before sending. The message is queued
        /// with the session's other commands, so it reaches the server in call order; the task
        /// completes once it is written to the transport (as centrifuge-js, no delivery confirmation)
        /// and fails with the write error, or when the session ends before the write completes. A token
        /// cancelled before it is queued fails the call without sending; cancelled later, it ends
        /// the wait, and the queued message may still be written.
        /// </summary>
        /// <param name="data">Message data.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public async Task SendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            await ReadyAsync(_options.Timeout, cancellationToken).ConfigureAwait(false);

            var cmd = new Command
            {
                Send = new SendRequest
                {
                    Data = ByteString.CopyFrom(data.Span)
                }
            };
            cancellationToken.ThrowIfCancellationRequested();
            await AwaitWriteAsync(QueueCommand(cmd, null, null), cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Gets presence information for a channel.
        /// Automatically waits for the client to be connected before sending.
        /// </summary>
        /// <param name="channel">Channel name.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Presence result.</returns>
        public async Task<CentrifugePresenceResult> PresenceAsync(string channel, CancellationToken cancellationToken = default)
        {
            // Wait for client to be ready
            await ReadyAsync(_options.Timeout, cancellationToken).ConfigureAwait(false);

            var cmd = new Command
            {
                Id = NextCommandId(),
                Presence = new PresenceRequest
                {
                    Channel = channel
                }
            };

            var reply = await SendCommandAsync(cmd, cancellationToken).ConfigureAwait(false);

            if (reply.Error != null)
            {
                throw CentrifugeException.FromReply(reply.Error);
            }

            var clients = new Dictionary<string, CentrifugeClientInfo>();
            foreach (var kvp in reply.Presence.Presence)
            {
                clients[kvp.Key] = CentrifugeClientInfo.FromProtocol(kvp.Value);
            }

            return new CentrifugePresenceResult(clients);
        }

        /// <summary>
        /// Gets presence stats for a channel.
        /// Automatically waits for the client to be connected before sending.
        /// </summary>
        /// <param name="channel">Channel name.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Presence stats result.</returns>
        public async Task<CentrifugePresenceStatsResult> PresenceStatsAsync(string channel, CancellationToken cancellationToken = default)
        {
            // Wait for client to be ready
            await ReadyAsync(_options.Timeout, cancellationToken).ConfigureAwait(false);

            var cmd = new Command
            {
                Id = NextCommandId(),
                PresenceStats = new PresenceStatsRequest
                {
                    Channel = channel
                }
            };

            var reply = await SendCommandAsync(cmd, cancellationToken).ConfigureAwait(false);

            if (reply.Error != null)
            {
                throw CentrifugeException.FromReply(reply.Error);
            }

            return new CentrifugePresenceStatsResult(
                reply.PresenceStats.NumClients,
                reply.PresenceStats.NumUsers
            );
        }

        /// <summary>
        /// Publishes data to a channel.
        /// This allows publishing to a channel without having a client-side subscription to it.
        /// Useful for server-side subscriptions or one-off publish operations.
        /// Automatically waits for the client to be connected before sending.
        /// </summary>
        /// <param name="channel">Channel name to publish to.</param>
        /// <param name="data">Data to publish.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public async Task PublishAsync(string channel, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            // Wait for client to be ready
            await ReadyAsync(_options.Timeout, cancellationToken).ConfigureAwait(false);

            var cmd = new Command
            {
                Id = NextCommandId(),
                Publish = new PublishRequest
                {
                    Channel = channel,
                    Data = ByteString.CopyFrom(data.Span)
                }
            };

            var reply = await SendCommandAsync(cmd, cancellationToken).ConfigureAwait(false);

            if (reply.Error != null)
            {
                throw CentrifugeException.FromReply(reply.Error);
            }
        }

        /// <summary>
        /// Gets channel history.
        /// This allows fetching history for a channel without having a client-side subscription to it.
        /// Useful for server-side subscriptions or one-off history requests.
        /// Automatically waits for the client to be connected before sending.
        /// By default, returns only current stream position data (no publications).
        /// To retrieve publications, provide an explicit limit > 0 in the options.
        /// </summary>
        /// <param name="channel">Channel name to get history for.</param>
        /// <param name="options">History options (limit, since position, reverse order).</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>History result with publications and stream position.</returns>
        public async Task<CentrifugeHistoryResult> HistoryAsync(string channel, CentrifugeHistoryOptions? options = null, CancellationToken cancellationToken = default)
        {
            // Wait for client to be ready
            await ReadyAsync(_options.Timeout, cancellationToken).ConfigureAwait(false);

            var request = new HistoryRequest
            {
                Channel = channel
            };

            if (options != null)
            {
                if (options.Limit.HasValue)
                {
                    request.Limit = options.Limit.Value;
                }

                if (options.Since != null)
                {
                    request.Since = new Centrifugal.Centrifuge.Protocol.StreamPosition
                    {
                        Offset = options.Since.Value.Offset,
                        Epoch = options.Since.Value.Epoch
                    };
                }

                request.Reverse = options.Reverse;
            }

            var cmd = new Command
            {
                Id = NextCommandId(),
                History = request
            };

            var reply = await SendCommandAsync(cmd, cancellationToken).ConfigureAwait(false);

            if (reply.Error != null)
            {
                throw CentrifugeException.FromReply(reply.Error);
            }

            var publications = new List<CentrifugePublicationEventArgs>();
            foreach (var pub in reply.History.Publications)
            {
                publications.Add(CreatePublicationArgs(channel, pub));
            }

            return new CentrifugeHistoryResult(
                publications.ToArray(),
                reply.History.Epoch,
                reply.History.Offset
            );
        }

        internal Task<Reply> SendCommandAsync(Command command, CancellationToken cancellationToken) =>
            SendCommandAsync(command, null, null, cancellationToken);

        /// <summary>
        /// Sends a command and returns its reply. <paramref name="session"/>, when given, binds the
        /// command to that transport: it fails unless the transport is still current. The command
        /// timeout runs from the write, not from queueing: a command behind earlier sends doesn't time
        /// out in the queue; a failed write fails the call with the write error. A token cancelled
        /// before the command is queued fails it without sending; once queued, it only stops the wait
        /// (a connect command's write is awaited to the end). <paramref name="onReply"/>, when given,
        /// is applied on the receive loop before the next message (see <see cref="NewPendingCall"/>);
        /// when the reply is returned, it has run.
        /// </summary>
        internal async Task<Reply> SendCommandAsync(Command command, ITransport? session, Action<Reply>? onReply,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var tcs = NewPendingCall(onReply);
            var written = QueueCommand(command, session, tcs);

            try
            {
                if (command.Connect != null)
                    await written.ConfigureAwait(false);
                else
                    await AwaitWriteAsync(written, cancellationToken).ConfigureAwait(false);

                return await AwaitReplyAsync(command.Id, tcs, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _pendingCalls.TryRemove(command.Id, out _);
            }
        }

        /// <summary>
        /// Registers <paramref name="pendingCall"/>, when given, and queues the command in one critical
        /// section with the teardown (DetachTransportLocked): a command of a torn-down session is never
        /// sent on the next one. Connect commands are sent at once (only over a non-emulation transport:
        /// the emulation connect goes with the open request), others batched. Returns the write.
        /// </summary>
        private Task QueueCommand(Command command, ITransport? session, TaskCompletionSource<Reply>? pendingCall)
        {
            var isConnectCommand = command.Connect != null;
            var size = isConnectCommand ? 0 : command.CalculateSize();
            var written = isConnectCommand ? null : new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            ITransport transport;
            lock (_stateChangeLock)
            {
                transport = QueueTransportLocked(session)
                    ?? throw new CentrifugeException(CentrifugeErrorCodes.ClientDisconnected, "Client is not connected");
                if (pendingCall != null) _pendingCalls[command.Id] = pendingCall;
                if (written != null) ScheduleCommandBatch(command, size, written);
            }

            return written?.Task ?? SendCommandsAsync(transport, null, new[] { command });
        }

        /// <summary>Awaits a queued write; the token only ends the wait, the write may still happen.</summary>
        private static async Task AwaitWriteAsync(Task written, CancellationToken cancellationToken)
        {
            if (!await Utilities.CompletesBeforeCancellationAsync(written, cancellationToken).ConfigureAwait(false))
                cancellationToken.ThrowIfCancellationRequested();
            await written.ConfigureAwait(false);
        }

        /// <summary>
        /// Awaits the reply of a pending call within the command timeout. Whoever removes a pending
        /// call completes it: one claimed by the receive loop or a teardown outlives the timeout, while
        /// the caller's cancellation, reported as such rather than as a timeout, holds until the reply.
        /// </summary>
        private async Task<Reply> AwaitReplyAsync(uint id, TaskCompletionSource<Reply> tcs, CancellationToken cancellationToken)
        {
            using (var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
            {
                timeoutCts.CancelAfter(_options.Timeout);
                if (!await Utilities.CompletesBeforeCancellationAsync(tcs.Task, timeoutCts.Token).ConfigureAwait(false))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (_pendingCalls.TryRemove(id, out _)) throw new CentrifugeTimeoutException();
                    await Utilities.CompletesBeforeCancellationAsync(tcs.Task, cancellationToken).ConfigureAwait(false);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            }
            return await tcs.Task.ConfigureAwait(false);
        }

        /// <summary>Where the commands of an emulation transport's session go.</summary>
        private readonly struct EmulationTarget
        {
            public EmulationTarget(string session, string node, string endpoint)
            {
                Session = session;
                Node = node;
                Endpoint = endpoint;
            }

            public string Session { get; }
            public string Node { get; }
            public string Endpoint { get; }
        }

        /// <summary>
        /// Sends the commands as one varint-delimited frame, or one emulation request to
        /// <paramref name="emulation"/> (the emulation connect goes with the open request instead,
        /// see CreateTransportAsync). The write is bounded by the command timeout, or by
        /// <paramref name="deadline"/> when the caller holds one; its failure is a temporary
        /// TransportWriteError carrying the cause, for the connect, the batches and the pong alike.
        /// </summary>
        private async Task SendCommandsAsync(ITransport transport, EmulationTarget? emulation, IEnumerable<Command> commands,
            CancellationToken? deadline = null)
        {
            var ownDeadline = deadline == null ? new CancellationTokenSource(_options.Timeout) : null;
            var token = deadline ?? ownDeadline!.Token;
            try
            {
                using var ms = new MemoryStream();
                foreach (var cmd in commands)
                {
                    VarintCodec.WriteDelimitedMessage(ms, cmd.ToByteArray());
                }
                var delimitedData = ms.ToArray();

                if (emulation is { } target)
                {
                    await transport.SendEmulationAsync(delimitedData, target.Session, target.Node, target.Endpoint, token).ConfigureAwait(false);
                }
                else
                {
                    await transport.SendAsync(delimitedData, token).ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (ex is not CentrifugeException { Code: CentrifugeErrorCodes.TransportWriteError })
            {
                throw new CentrifugeException(CentrifugeErrorCodes.TransportWriteError, "transport write error", true, ex);
            }
            finally
            {
                ownDeadline?.Dispose();
            }
        }

        /// <summary>Takes the queued commands of one send: up to MaxCommandBatchSize, at least one, and arms
        /// its deadline (<see cref="_writingDeadline"/>). Call under _commandBatchLock.</summary>
        private List<(Command Command, TaskCompletionSource<bool> Written)> TakeCommandBatchLocked()
        {
            int count = 0;
            int bytes = 0;
            while (count < _commandBatch.Count && (count == 0 || bytes + _commandBatch[count].Size <= MaxCommandBatchSize))
            {
                bytes += _commandBatch[count].Size;
                count++;
            }

            var commands = new List<(Command Command, TaskCompletionSource<bool> Written)>(count);
            for (int i = 0; i < count; i++) commands.Add((_commandBatch[i].Command, _commandBatch[i].Written));
            _commandBatch.RemoveRange(0, count);
            _commandBatchSize -= bytes;
            _commandBatchPending = false;
            _writingDeadline = new CancellationTokenSource(_options.Timeout);
            return commands;
        }

        /// <summary>
        /// Queues a command for a flush: the timer's after CommandBatchDelayMs, or one at once when the
        /// queue reaches MaxCommandBatchSize. One flush is scheduled until it takes the queue.
        /// </summary>
        /// <param name="command">The command.</param>
        /// <param name="size">Its encoded size, computed before the caller took the state lock.</param>
        /// <param name="written">Completed once the command is written, faulted by a write error or when
        /// its session ends before the write completes.</param>
        private void ScheduleCommandBatch(Command command, int size, TaskCompletionSource<bool> written)
        {
            lock (_commandBatchLock)
            {
                size += 10; // varint overhead
                _commandBatch.Add((command, size, written));
                _commandBatchSize += size;

                if (_commandBatchSize >= MaxCommandBatchSize)
                {
                    if (_commandBatchPending && _commandBatchTimer == null) return;

                    _commandBatchTimer?.Dispose();
                    _commandBatchTimer = null;
                    _commandBatchPending = true;
                    _ = Task.Run(FlushCommandBatchAsync);
                    return;
                }

                if (!_commandBatchPending)
                {
                    _commandBatchPending = true;
                    Timer? timer = null;
                    timer = new Timer(_ =>
                    {
                        lock (_commandBatchLock)
                        {
                            if (ReferenceEquals(_commandBatchTimer, timer)) _commandBatchTimer = null;
                        }
                        timer!.Dispose();
                        _ = FlushCommandBatchAsync();
                    }, null, TimeSpan.FromMilliseconds(CommandBatchDelayMs), System.Threading.Timeout.InfiniteTimeSpan);
                    _commandBatchTimer = timer;
                }
            }
        }

        /// <summary>
        /// Sends the queued commands of the current transport session on that transport, one
        /// send at a time per session, so they reach the server in queue order even when
        /// flushes overlap (see SetUnsubscribedAsync). A send takes up to MaxCommandBatchSize —
        /// what queued meanwhile goes in the next ones. The outcome of a send belongs to its write: a write
        /// that completed stays written even when its session ended meanwhile, while the teardown cancels
        /// one still in progress (<see cref="_writingDeadline"/>) — its commands fail as ended with the
        /// session. A failed emulation request ends the session before its commands learn of it — they fail
        /// with the teardown, not as errors of a live session: the stream stays open, and a later request
        /// must not overtake one whose outcome is unknown (past the command timeout it may still reach the
        /// server). A failed WebSocket send closes the transport (see ITransport.SendAsync); its
        /// receive path ends the session — with the server's reason when its close arrived first —
        /// while the flush goes on: the rest of the queue fails at once on the closed transport rather
        /// than waiting for that. A flush that outwaited its session sends nothing: its commands were
        /// dropped with it.
        /// </summary>
        private async Task FlushCommandBatchAsync()
        {
            ITransport? transport;
            SemaphoreSlim flushLock;
            lock (_stateChangeLock)
            {
                if (_state != CentrifugeClientState.Connected) return;
                transport = _transport;
                flushLock = _flushLock;
            }
            if (transport == null) return;

            await flushLock.WaitAsync().ConfigureAwait(false);
            try
            {
                while (true)
                {
                    List<(Command Command, TaskCompletionSource<bool> Written)> commandsToSend;
                    CancellationTokenSource deadline;
                    CancellationToken deadlineToken;
                    EmulationTarget? emulation;

                    lock (_stateChangeLock)
                    {
                        if (_state != CentrifugeClientState.Connected || !ReferenceEquals(_transport, transport)) return;
                        emulation = transport.UsesEmulation ? new EmulationTarget(_session, _node, _emulationEndpoint!) : null;

                        lock (_commandBatchLock)
                        {
                            if (_commandBatch.Count == 0) return;
                            commandsToSend = TakeCommandBatchLocked();
                            deadline = _writingDeadline!;
                            deadlineToken = deadline.Token;
                        }
                    }

                    try
                    {
                        await SendCommandsAsync(transport, emulation, commandsToSend.Select(entry => entry.Command), deadlineToken)
                            .ConfigureAwait(false);
                    }
                    catch (CentrifugeException writeError)
                    {
                        _logger?.LogDebug($"Error flushing command batch: {writeError.InnerException?.Message}");
                        if (transport.UsesEmulation)
                        {
                            ReconnectSession(() => _state == CentrifugeClientState.Connected && ReferenceEquals(_transport, transport),
                                CentrifugeConnectingCodes.TransportClosed, "send failed");
                        }
                        bool sessionEnded;
                        lock (_commandBatchLock) sessionEnded = !ReferenceEquals(_writingDeadline, deadline);
                        var error = sessionEnded ? ConnectionClosedError() : writeError;
                        foreach (var entry in commandsToSend) FailObserved(entry.Written, error);
                        if (sessionEnded) return;
                        continue;
                    }
                    finally
                    {
                        bool owned;
                        lock (_commandBatchLock)
                        {
                            owned = ReferenceEquals(_writingDeadline, deadline);
                            if (owned) _writingDeadline = null;
                        }
                        if (owned) deadline.Dispose();
                    }
                    foreach (var entry in commandsToSend) entry.Written.TrySetResult(true);
                }
            }
            finally
            {
                flushLock.Release();
            }
        }

        /// <summary>
        /// Creates the transport of <paramref name="endpoint"/> with what an emulation transport needs
        /// before it opens: its emulation endpoint and connect command.
        /// </summary>
        private (ITransport Transport, string? EmulationEndpoint, Command? ConnectCommand) PrepareTransport(
            CentrifugeTransportType type, string endpoint)
        {
            var transport = CreateTransport(type, endpoint);
            return transport.UsesEmulation
                ? (transport, EmulationEndpointFor(endpoint), BuildConnectCommand())
                : (transport, null, null);
        }

        /// <summary>The emulation endpoint of a transport created for <paramref name="endpoint"/>: the
        /// configured one, or the root-level /emulation of its host — of the page for a relative browser
        /// endpoint.</summary>
        private string EmulationEndpointFor(string endpoint)
        {
            if (!string.IsNullOrEmpty(_options.EmulationEndpoint))
            {
                return _options.EmulationEndpoint!;
            }

            return IsHttpUrl(endpoint)
                ? $"{new Uri(endpoint).GetLeftPart(UriPartial.Authority)}/emulation"
                : "/emulation";
        }

        /// <summary>
        /// Moves a Disconnected client to Connecting and starts the attempt. The disposal and state checks
        /// and the transition are one critical section: a concurrent Disconnect() isn't overwritten, and a
        /// concurrent Dispose() either makes this throw or disconnects the Connecting client.
        /// </summary>
        private void StartConnecting()
        {
            CentrifugeClientState prevState;
            int epoch;
            lock (_stateChangeLock)
            {
                ThrowIfDisposed();
                if (_state != CentrifugeClientState.Disconnected) return;
                _reconnectAttempts = 0;
                prevState = SetState(CentrifugeClientState.Connecting);
                epoch = _epoch;
            }
            Raise(StateChanged, new CentrifugeStateEventArgs(prevState, CentrifugeClientState.Connecting), "stateChanged");
            if (IsEpoch(epoch))
                Raise(Connecting, new CentrifugeConnectingEventArgs(CentrifugeConnectingCodes.ConnectCalled, "connect called"), "connecting");

            _ = Task.Run(async () =>
            {
                try
                {
                    await CreateTransportAsync(epoch).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    await HandleConnectAttemptErrorAsync(ex, epoch).ConfigureAwait(false);
                }
            });
        }

        /// <summary>Handles a failure of the first connect attempt (see ReportConnectAttemptErrorAsync) and
        /// starts the reconnect without waiting for it.</summary>
        private async Task HandleConnectAttemptErrorAsync(Exception ex, int epoch)
        {
            if (await ReportConnectAttemptErrorAsync(ex, epoch).ConfigureAwait(false))
                _ = ScheduleReconnectAsync(epoch);
        }

        /// <summary>
        /// Handles a failure of CreateTransportAsync, on the first connect and on every reconnect
        /// alike: unauthorized stops connecting for good, anything else is reported — a connection
        /// token failure as "connectToken" (as centrifuge-js), others as "transport" — and true returned for the
        /// next attempt, unless the connect attempt identified by <paramref name="epoch"/> was
        /// superseded (Disconnect(), maybe followed by Connect()), which the failure must not affect.
        /// </summary>
        private async Task<bool> ReportConnectAttemptErrorAsync(Exception ex, int epoch)
        {
            if (ex is CentrifugeUnauthorizedException)
            {
                await SetDisconnectedAsync(CentrifugeDisconnectedCodes.Unauthorized, "unauthorized", epoch: epoch).ConfigureAwait(false);
                return false;
            }

            lock (_stateChangeLock)
            {
                if (_epoch != epoch) return false;
            }
            OnError(ex is CentrifugeException { Code: CentrifugeErrorCodes.ClientConnectToken } ? "connectToken" : "transport", ex);
            return true;
        }

        /// <summary>An attempt ended while its transport hadn't proved to reach the server (_transportWasOpen):
        /// fallback mode moves to the next endpoint (as centrifuge-js) — for an open failure, a connect
        /// without reply, an unparsable frame and a close alike. Call under _stateChangeLock.</summary>
        private void AdvanceUnprovenTransportLocked()
        {
            if (!_transportWasOpen && _transportEndpoints != null)
                _currentTransportIndex = (_currentTransportIndex + 1) % _transportEndpoints.Count;
        }

        /// <summary>
        /// Creates and opens a transport for the connect attempt identified by
        /// <paramref name="epoch"/> — the state epoch of the Connecting state it runs in. The
        /// connection token is got first, before any transport exists (as centrifuge-js): its
        /// failure retries the same endpoint without opening one. The transport
        /// becomes the current one only while that attempt is current. Emulation transports send the connect command
        /// with the open request: it is installed together with the transport. A transport that
        /// fails to open within OpenTimeout is dropped with its pending calls, which unhooks its
        /// events so async JS callbacks (onclose etc.) don't start a second reconnect; while no
        /// transport has opened yet, the next attempt tries the next fallback endpoint.
        /// </summary>
        private async Task CreateTransportAsync(int epoch)
        {
            if (!await GetConnectTokenIfNeededAsync(epoch).ConfigureAwait(false)) return;

            CentrifugeTransportType type;
            string endpoint;
            if (_transportEndpoints != null)
            {
                int idx;
                lock (_stateChangeLock) { idx = _currentTransportIndex; }
                (type, endpoint) = (_transportEndpoints[idx].Transport, _transportEndpoints[idx].Endpoint);
            }
            else
            {
                (type, endpoint) = (_endpointTransport, _endpoint!);
            }
            var (transport, emulationEndpoint, connectCommand) = PrepareTransport(type, endpoint);

            bool installed;
            lock (_stateChangeLock)
            {
                installed = _epoch == epoch;
                if (installed)
                {
                    _transport = transport;
                    _emulationEndpoint = emulationEndpoint;
                    if (connectCommand != null)
                    {
                        var call = NewPendingCall(reply => HandleConnectReply(transport, connectCommand.Connect.Token, reply));
                        _pendingCalls[connectCommand.Id] = call;
                        _pendingConnectCall = (connectCommand.Id, call);
                    }
                }
            }
            if (!installed)
            {
                transport.Dispose();
                return;
            }

            transport.Opened += OnTransportOpened;
            transport.MessageReceived += OnTransportMessage;
            transport.Closed += OnTransportClosed;
            transport.Error += OnTransportError;

            try
            {
                using var openTimeout = new CancellationTokenSource(_options.OpenTimeout);
                await transport.OpenAsync(openTimeout.Token, connectCommand?.ToByteArray()).ConfigureAwait(false);
            }
            catch
            {
                DropTransport(transport);
                throw;
            }
        }

        /// <summary>The failure of a call whose connection went away before its reply.</summary>
        private static CentrifugeException ConnectionClosedError() =>
            new CentrifugeException(CentrifugeErrorCodes.ConnectionClosed, "connection closed", false);

        private ITransport CreateTransport(CentrifugeTransportType transportType, string endpoint)
        {
            switch (transportType)
            {
                case CentrifugeTransportType.WebSocket:
#if NET6_0_OR_GREATER
                    if (_jsRuntime != null) return new BrowserWebSocketTransport(endpoint, _jsRuntime, _logger);
#endif
                    return new WebSocketTransport(endpoint);
                case CentrifugeTransportType.HttpStream:
#if NET6_0_OR_GREATER
                    if (_jsRuntime != null) return new BrowserHttpStreamTransport(endpoint, _jsRuntime, _logger);
#endif
                    return new HttpStreamTransport(endpoint);
                default:
                    throw new CentrifugeConfigurationException($"Unsupported transport type: {transportType}");
            }
        }

        /// <summary>
        /// Sends the connect of the attempt the opened transport belongs to. The transport and
        /// the state epoch identify the attempt: a failure surfacing after the app disconnected
        /// (maybe connected again) belongs to a replaced transport and neither tears down the
        /// current one nor reconnects or disconnects the client.
        /// </summary>
        private async void OnTransportOpened(object? sender, EventArgs e)
        {
            _logger?.LogDebug("OnTransportOpened called");
            // Defensive check: don't process events if client is disposed
            if (_disposed != 0) return;
            if (!(sender is ITransport transport)) return;
            int epoch;
            lock (_stateChangeLock)
            {
                if (!ReferenceEquals(_transport, transport)) return;
                epoch = _epoch;
                if (!transport.UsesEmulation) _transportWasOpen = true;
            }

            try
            {
                _logger?.LogDebug("Sending connect command...");
                await SendConnectCommandAsync(transport).ConfigureAwait(false);
                _logger?.LogDebug("Connect reply received");
            }
            catch (Exception ex)
            {
                _logger?.LogDebug($"Connect failed: {ex.GetType().Name}: {ex.Message}");
                await FailConnectAsync(transport, epoch, ex).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Handles a failed connect of the current <paramref name="transport"/> — before its reply was
        /// applied: a permanent error disconnects and anything else drops the attempt and schedules the
        /// next one. A failed connect write is left to the transport it closes (see
        /// ITransport.SendAsync): its Closed ends the attempt, with the server's close code when one
        /// arrived.
        /// </summary>
        private async Task FailConnectAsync(ITransport transport, int epoch, Exception error)
        {
            if (error is CentrifugeException { Code: CentrifugeErrorCodes.TransportWriteError }) return;

            lock (_stateChangeLock)
            {
                if (!ReferenceEquals(_transport, transport)) return;
            }

            if (error is CentrifugeException { Temporary: false } permanent && permanent.Code >= 100 && permanent.Code != 109)
            {
                OnError("connect", permanent);
                await SetDisconnectedAsync(permanent.Code, permanent.Message, epoch: epoch).ConfigureAwait(false);
                return;
            }

            if (!DropTransport(transport)) return;
            OnError("connect", error is CentrifugeTimeoutException
                ? new CentrifugeException(CentrifugeErrorCodes.Timeout, "connect timeout", true)
                : error);
            _ = ScheduleReconnectAsync(epoch);
        }

        /// <summary>
        /// Releases (ReleaseSessionLocked) and disposes <paramref name="transport"/> after a failed
        /// connect attempt (AdvanceUnprovenTransportLocked). Returns false, doing nothing, when it is no
        /// longer the current transport (a newer attempt replaced it).
        /// </summary>
        private bool DropTransport(ITransport transport)
        {
            CancellationTokenSource? write;
            lock (_stateChangeLock)
            {
                if (!ReferenceEquals(_transport, transport)) return false;
                AdvanceUnprovenTransportLocked();
                ReleaseSessionLocked(out write);
            }
            CancelWrite(write);
            UnhookTransport(transport);
            transport.Dispose();
            return true;
        }

        private void UnhookTransport(ITransport transport)
        {
            transport.Opened -= OnTransportOpened;
            transport.MessageReceived -= OnTransportMessage;
            transport.Closed -= OnTransportClosed;
            transport.Error -= OnTransportError;
        }

        /// <summary>
        /// Gets the connection token of the attempt identified by <paramref name="epoch"/> when there
        /// is none or it must be refreshed. Returns false when the attempt was superseded — before
        /// GetToken is called or while it is awaited. A GetToken failure other than Unauthorized is a
        /// temporary ClientConnectToken error (as centrifuge-js): retried with backoff, never taken
        /// for a server reply. A null token is Unauthorized, an empty one connects without a token; an
        /// expired token (109) that no GetToken can replace is a configuration error, then Unauthorized
        /// (as centrifuge-js).
        /// </summary>
        private async Task<bool> GetConnectTokenIfNeededAsync(int epoch)
        {
            bool expired;
            bool needed;
            lock (_stateChangeLock)
            {
                if (_epoch != epoch) return false;
                expired = _refreshRequired;
                needed = string.IsNullOrEmpty(_token) || expired;
            }
            if (!needed) return true;
            if (_options.GetToken == null)
            {
                if (!expired) return true;
                OnError("configuration", new CentrifugeConfigurationException("token expired but no GetToken is set"));
                throw new CentrifugeUnauthorizedException();
            }

            string token;
            try
            {
                token = await _options.GetToken().ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not CentrifugeUnauthorizedException)
            {
                throw new CentrifugeException(CentrifugeErrorCodes.ClientConnectToken, ex.Message, true, ex);
            }
            if (token == null) throw new CentrifugeUnauthorizedException();
            lock (_stateChangeLock)
            {
                if (_epoch != epoch) return false;
                _token = token;
            }
            return true;
        }

        /// <summary>Builds the connect command with the token got for the attempt (see
        /// GetConnectTokenIfNeededAsync).</summary>
        private Command BuildConnectCommand()
        {
            string? token;
            ReadOnlyMemory<byte> data;
            Dictionary<string, string>? headers;
            (long Seq, string Channel, ServerSubscription Base, ulong Offset, string Epoch)? delivering;
            lock (_stateChangeLock)
            {
                token = _token;
                data = _data;
                headers = _headers;
                delivering = _deliveringServerPublication;
            }

            var connectRequest = new ConnectRequest
            {
                Token = token ?? string.Empty,
                Name = _options.Name,
                Version = _options.Version
            };

            if (!data.IsEmpty)
            {
                connectRequest.Data = ByteString.CopyFrom(data.Span);
            }

            if (headers != null && headers.Count > 0)
            {
                foreach (var kvp in headers)
                {
                    connectRequest.Headers.Add(kvp.Key, kvp.Value);
                }
            }

            // Include server subscriptions for recovery
            foreach (var kvp in _serverSubscriptions)
            {
                var channel = kvp.Key;
                var serverSub = kvp.Value;
                if (delivering is { } d && d.Channel == channel && ReferenceEquals(d.Base, serverSub.Base))
                    serverSub = serverSub.Past(d.Offset, d.Epoch) ?? serverSub;

                if (serverSub.Recoverable)
                {
                    var subRequest = new Centrifugal.Centrifuge.Protocol.SubscribeRequest
                    {
                        Channel = channel,
                        Recover = true,
                        Offset = serverSub.Offset,
                        Epoch = serverSub.Epoch
                    };
                    connectRequest.Subs.Add(channel, subRequest);
                }
            }

            var cmd = new Command
            {
                Id = NextCommandId(),
                Connect = connectRequest
            };

            return cmd;
        }

        /// <summary>
        /// Sends the connect of the attempt on <paramref name="transport"/> and waits for its reply,
        /// which is applied on the receive loop, an error one too (<see cref="HandleConnectReply"/>).
        /// An emulation transport sent it with the open request, its call registered together with
        /// the transport: only the reply is awaited. A failed send or wait throws.
        /// </summary>
        private async Task SendConnectCommandAsync(ITransport transport)
        {
            _logger?.LogDebug($"SendConnectCommandAsync - UsesEmulation: {transport.UsesEmulation}");
            if (transport.UsesEmulation)
            {
                (uint Id, TaskCompletionSource<Reply> Call) connect;
                lock (_stateChangeLock)
                {
                    if (!ReferenceEquals(_transport, transport)) return;
                    connect = _pendingConnectCall!.Value;
                    _pendingConnectCall = null;
                }

                await AwaitReplyAsync(connect.Id, connect.Call, CancellationToken.None).ConfigureAwait(false);
            }
            else
            {
                var cmd = BuildConnectCommand();
                _logger?.LogDebug($"Sending connect command with ID: {cmd.Id}");
                await SendCommandAsync(cmd, transport, r => HandleConnectReply(transport, cmd.Connect.Token, r), CancellationToken.None).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Applies a connect reply. Runs on the receive loop of the transport the connect was sent
        /// on, before any message that follows the reply — so the server-side subscription events
        /// and recovered publications it carries precede the live publications, and an error reply
        /// fails the attempt (FailConnectAsync; 109 also marks <paramref name="sentToken"/> for refresh,
        /// unless SetToken replaced it meanwhile — as centrifuge-js) while it is
        /// still current: the server closes the connection after it, and that close would end the
        /// attempt first. A reply to a connect the app superseded (disconnected, maybe connected
        /// again) is stale and ignored. One critical section spans the field writes, the
        /// transition, the resolve of the ready waiters and the server-side subscriptions: a ReadyAsync caller can't
        /// observe Connecting, miss the resolve and register a promise nobody completes. The events
        /// stop once the client moved on (a
        /// handler disconnected). The subscribe commands of Subscribing subscriptions follow, and
        /// the batch is flushed: commands queued on this transport while Connecting (calls that passed
        /// ReadyAsync before the reconnect) would otherwise wait for the next flush trigger.
        /// </summary>
        private void HandleConnectReply(ITransport transport, string sentToken, Reply reply)
        {
            if (reply.Error != null)
            {
                int epoch;
                lock (_stateChangeLock)
                {
                    if (!ReferenceEquals(_transport, transport)) return;
                    _transportWasOpen = true;
                    if (reply.Error.Code == 109 && sentToken == (_token ?? string.Empty)) _refreshRequired = true;
                    epoch = _epoch;
                }
                _ = FailConnectAsync(transport, epoch, CentrifugeException.FromReply(reply.Error));
                return;
            }
            var connectResult = reply.Connect ?? throw new InvalidDataException("connect reply without result");

            string transportName;
            CentrifugeClientState prevState;
            int connectedEpoch;
            HashSet<string> newServerSubs;
            List<string> removedServerSubs;
            Dictionary<string, ServerSubscription?> keptServerSubs;
            lock (_stateChangeLock)
            {
                if (_state != CentrifugeClientState.Connecting || !ReferenceEquals(_transport, transport)) return;
                _session = connectResult.Session;
                _node = connectResult.Node;
                prevState = SetState(CentrifugeClientState.Connected);
                _readyPromises.ResolveAll();

                _transportWasOpen = true;
                if (transport.UsesEmulation) _reconnectAttemptsResetPending = true;
                else _reconnectAttempts = 0;
                ClearRefreshTimer();
                _refreshAttempts = 0;
                _refreshRequired = false;
                if (connectResult.Expires) ScheduleConnectionRefresh(connectResult.Ttl);

                if (connectResult.Ping > 0)
                {
                    _serverPingInterval = connectResult.Ping;
                    _sendPong = connectResult.Pong;
                    StartPingTimer(connectResult.Ping, transport);
                }
                else
                {
                    _serverPingInterval = 0;
                    _sendPong = false;
                }

                transportName = transport.Name;
                connectedEpoch = _epoch;
                ApplyServerSubscriptionsLocked(connectResult.Subs, out newServerSubs, out removedServerSubs, out keptServerSubs);
            }

            Raise(StateChanged, new CentrifugeStateEventArgs(prevState, CentrifugeClientState.Connected), "stateChanged");
            if (IsCurrentSession(connectedEpoch))
            {
                if (Connected != null)
                {
                    Raise(Connected, new CentrifugeConnectedEventArgs(
                        connectResult.Client,
                        transportName,
                        connectResult.Data.ToByteArray()
                    ), "connected");
                }
                RaiseServerSubscriptionEvents(connectResult.Subs, newServerSubs, removedServerSubs, keptServerSubs, connectedEpoch);
            }

            ScheduleSubscribeBatch();
            _ = Task.Run(FlushCommandBatchAsync);
        }

        /// <summary>
        /// Starts, off the calling thread, the subscribe attempts of all subscriptions while
        /// Connected; each checks its own state (SendSubscribeIfNeededAsync), and the general
        /// command batching groups their commands. The attempts aren't awaited: each runs its own
        /// retry loop.
        /// </summary>
        private void ScheduleSubscribeBatch() => _ = Task.Run(SendSubscribeCommands);

        private void SendSubscribeCommands()
        {
            lock (_stateChangeLock)
            {
                if (_state != CentrifugeClientState.Connected) return;
            }

            foreach (var sub in _subscriptions.Values)
            {
                _ = sub.SendSubscribeIfNeededAsync();
            }
        }

        /// <summary>
        /// Dispatches the messages of a received frame in order. The replies of the whole frame
        /// are claimed first, so one already received can't time out while handlers of an
        /// earlier message run (whoever removes a pending call completes it; a claimed reply
        /// left undispatched fails like a torn-down session's call). A message that fails to
        /// parse or apply is reported at its place in the frame (see FailFrame). Dispatch stops
        /// once a handler ended the session.
        /// </summary>
        private void OnTransportMessage(object? sender, IReadOnlyList<byte[]> messages)
        {
            // Defensive check: don't process events if client is disposed
            if (_disposed != 0 || !(sender is ITransport transport) || !IsDispatching(transport)) return;

            var replies = new Reply?[messages.Count];
            TaskCompletionSource<Reply>?[]? claimed = null;
            Exception?[]? parseErrors = null;
            try
            {
                using var dispatch = new DispatchScope(transport);
                for (var i = 0; i < messages.Count; i++)
                {
                    try
                    {
                        var reply = Reply.Parser.ParseFrom(messages[i]);
                        replies[i] = reply;
                        if (reply.Id > 0 && _pendingCalls.TryRemove(reply.Id, out var call))
                        {
                            (claimed ??= new TaskCompletionSource<Reply>?[messages.Count])[i] = call;
                        }
                    }
                    catch (Exception ex)
                    {
                        (parseErrors ??= new Exception?[messages.Count])[i] = ex;
                    }
                }

                for (var i = 0; i < replies.Length; i++)
                {
                    if (!IsDispatching(transport)) break;
                    if (parseErrors?[i] is { } parseError)
                    {
                        FailFrame(transport, parseError);
                        break;
                    }
                    var reply = replies[i]!;
                    try
                    {
                        ResetPingTimer();
                        HandleReply(reply, claimed?[i]);
                        if (claimed != null) claimed[i] = null;
                    }
                    catch (Exception ex)
                    {
                        FailFrame(transport, ex);
                        break;
                    }
                }
            }
            finally
            {
                if (claimed != null)
                {
                    Exception? connClosedEx = null;
                    foreach (var call in claimed)
                    {
                        if (call != null) FailObserved(call, connClosedEx ??= ConnectionClosedError());
                    }
                }
            }
        }

        /// <summary>
        /// A message of <paramref name="transport"/>'s frame broke the protocol: Error("parse"), and its
        /// session reconnects as after a malformed frame (as centrifuge-js), so nothing past the lost
        /// message is dispatched — recovery resumes before it.
        /// </summary>
        private void FailFrame(ITransport transport, Exception error)
        {
            OnError("parse", error);
            ReconnectSession(() => ReferenceEquals(_transport, transport),
                CentrifugeConnectingCodes.TransportClosed, "transport closed", advanceUnopenedTransport: true);
        }

        /// <summary>
        /// Whether messages of <paramref name="transport"/> are still dispatched: it is the
        /// current transport. Checked before each message, so once a teardown detached it (see
        /// DetachTransportLocked) no further message is dispatched; one whose dispatch already
        /// started when a teardown ran on another thread completes. Disposal stops it the same way:
        /// the detach comes with the session's end, so a claimed reply failed after it belongs to an
        /// attempt that is no longer current.
        /// </summary>
        private bool IsDispatching(ITransport transport) => ReferenceEquals(Volatile.Read(ref _transport), transport);

        /// <summary>
        /// The transport whose receive loop is dispatching an event on the current
        /// thread. A cleanup started from inside that dispatch (e.g. a synchronous
        /// Dispose() in an event handler) must not wait for the loop to exit. A nested
        /// dispatch on the same thread restores the outer one when it ends.
        /// </summary>
        [ThreadStatic]
        private static ITransport? t_dispatchingTransport;

        /// <summary>
        /// Marks the current thread as dispatching the events of a transport (t_dispatchingTransport)
        /// until disposed, which restores the outer dispatch. A struct: no allocation on the receive path.
        /// </summary>
        private readonly struct DispatchScope : IDisposable
        {
            private readonly ITransport? _outer;

            public DispatchScope(ITransport transport)
            {
                _outer = t_dispatchingTransport;
                t_dispatchingTransport = transport;
            }

            public void Dispose() => t_dispatchingTransport = _outer;
        }

        /// <summary>
        /// The transport a command can be queued on (bound to <paramref name="session"/> when given), or
        /// null: Connected, or Connecting with a transport installed — the queue is flushed once
        /// connected. Call under _stateChangeLock.
        /// </summary>
        private ITransport? QueueTransportLocked(ITransport? session) =>
            _transport != null && (session == null || ReferenceEquals(_transport, session))
            && _state is CentrifugeClientState.Connected or CentrifugeClientState.Connecting
                ? _transport
                : null;

        /// <summary>
        /// Creates the completion source a command's reply is delivered to.
        /// <paramref name="onReply"/>, carried as the task's AsyncState, is invoked by
        /// HandleReply synchronously on the transport receive loop before the task
        /// completes — so the state changes and events a reply produces happen before
        /// the next message of the same transport is processed. Awaiters of the task are
        /// still resumed asynchronously.
        /// </summary>
        private static TaskCompletionSource<Reply> NewPendingCall(Action<Reply>? onReply) =>
            new TaskCompletionSource<Reply>(onReply, TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>
        /// Faults a pending call or write with its exception marked observed: one nobody awaits any more
        /// (an emulation connect whose transport never opened, a command whose send failed first, a
        /// SendAsync its caller stopped waiting for) doesn't surface as an unobserved task exception,
        /// while an awaiter still gets it.
        /// </summary>
        private static void FailObserved<T>(TaskCompletionSource<T> completion, Exception error)
        {
            if (completion.TrySetException(error)) _ = completion.Task.Exception;
        }

        /// <summary>A server ping or a command reply (a late one too) of the current transport proves its
        /// emulation session steady: the backoff reset its connect reply deferred takes place
        /// (_reconnectAttemptsResetPending).</summary>
        private void ConfirmSessionSteady()
        {
            if (!Volatile.Read(ref _reconnectAttemptsResetPending)) return;
            lock (_stateChangeLock)
            {
                if (!_reconnectAttemptsResetPending || !ReferenceEquals(_transport, t_dispatchingTransport)) return;
                _reconnectAttemptsResetPending = false;
                _reconnectAttempts = 0;
            }
        }

        /// <param name="reply">The reply or push.</param>
        /// <param name="call">The pending call the receive loop claimed for a reply, or null
        /// when none was pending (timed out, torn down, unknown).</param>
        private void HandleReply(Reply reply, TaskCompletionSource<Reply>? call)
        {
            if (reply.Id > 0)
            {
                ConfirmSessionSteady();
                if (call == null) return;
                try
                {
                    (call.Task.AsyncState as Action<Reply>)?.Invoke(reply);
                }
                catch (Exception ex)
                {
                    FailObserved(call, ex);
                    return;
                }
                call.TrySetResult(reply);
            }
            else if (reply.Push != null)
            {
                // This is a server push
                HandlePush(reply.Push);
            }
            else
            {
                // Empty reply - this is a server ping
                HandleServerPing();
            }
        }

        /// <summary>
        /// Update the channel compaction registry for a subscription: remove the old
        /// numeric ID mapping (only if it still points to this subscription) and
        /// register the new one, assigned in Connected session <paramref name="connectionGeneration"/>
        /// — only while it is current, in one critical section with the teardown that clears the
        /// registry: an ID of an ended session doesn't take over one the next session assigned. Either
        /// ID may be 0 meaning "no mapping". Returns whether the new ID is registered.
        /// </summary>
        internal bool UpdateSubscriptionPushId(CentrifugeSubscription sub, long oldId, long newId, long connectionGeneration)
        {
            lock (_stateChangeLock)
            {
                if (oldId > 0)
                {
                    // Conditional removal: don't evict another subscription's registration
                    // after a cross-session ID collision. The TryRemove(KeyValuePair) overload
                    // isn't available on netstandard2.1, so go through ICollection.Remove which
                    // performs the same key+value conditional removal.
                    ((ICollection<KeyValuePair<long, CentrifugeSubscription>>)_subscriptionsByPushId)
                        .Remove(new KeyValuePair<long, CentrifugeSubscription>(oldId, sub));
                }
                if (newId == 0 || _connectionGeneration != connectionGeneration) return false;

                _subscriptionsByPushId[newId] = sub;
                return true;
            }
        }

        /// <summary>
        /// Resolve a client-side subscription for a push: by numeric channel ID when
        /// channel compaction is in use (the push then has no channel name), by
        /// channel name otherwise.
        /// </summary>
        private CentrifugeSubscription? ResolveSubscriptionForPush(string channel, long id)
        {
            if (id > 0)
            {
                _subscriptionsByPushId.TryGetValue(id, out var byId);
                return byId;
            }
            _subscriptions.TryGetValue(channel, out var byChannel);
            return byChannel;
        }

        /// <summary>
        /// A push no Subscribed client-side subscription takes belongs to a server-side one only if it
        /// is addressed by name (server-side subscriptions never use compaction) to a channel of the
        /// registry, as centrifuge-js: pushes in flight for a removed client-side subscription or a
        /// compacted ID it held are dropped, while an inactive client-side object of the channel doesn't
        /// shadow its server-side subscription.
        /// </summary>
        private bool IsServerSubscriptionPush(Push push) =>
            push.Id == 0 && _serverSubscriptions.ContainsKey(push.Channel);

        private void HandlePush(Push push)
        {
            if (push.Pub != null)
            {
                if (ResolveSubscriptionForPush(push.Channel, push.Id)?.HandlePublication(push.Pub) != true
                    && IsServerSubscriptionPush(push))
                {
                    _serverSubscriptions.TryGetValue(push.Channel, out ServerSubscription? delivered);
                    DeliverServerPublication(push.Channel, delivered, push.Pub);
                }
            }
            else if (push.Join != null)
            {
                if (ResolveSubscriptionForPush(push.Channel, push.Id)?.HandleJoin(push.Join) != true
                    && IsServerSubscriptionPush(push))
                {
                    if (push.Join.Info != null && Join != null)
                    {
                        Raise(Join, new CentrifugeJoinEventArgs(push.Channel, CentrifugeClientInfo.FromProtocol(push.Join.Info)), "join");
                    }
                }
            }
            else if (push.Leave != null)
            {
                if (ResolveSubscriptionForPush(push.Channel, push.Id)?.HandleLeave(push.Leave) != true
                    && IsServerSubscriptionPush(push))
                {
                    if (push.Leave.Info != null && Leave != null)
                    {
                        Raise(Leave, new CentrifugeLeaveEventArgs(push.Channel, CentrifugeClientInfo.FromProtocol(push.Leave.Info)), "leave");
                    }
                }
            }
            else if (push.Message != null)
            {
                if (Message != null)
                    Raise(Message, new CentrifugeMessageEventArgs(push.Message.Data.ToByteArray()), "message");
            }
            else if (push.Disconnect != null)
            {
                _logger?.LogDebug($"Received Disconnect push - code: {push.Disconnect.Code}, reason: '{push.Disconnect.Reason}'");
                HandleDisconnectPush(push.Disconnect);
            }
            else if (push.Subscribe != null)
            {
                HandleServerSubscribe(push.Channel, push.Subscribe);
            }
            else if (push.Unsubscribe != null)
            {
                HandleServerUnsubscribe(push.Channel, push.Unsubscribe);
            }
        }

        /// <summary>
        /// A Disconnect push ends the session of the transport it arrived on (the one being
        /// dispatched), not whatever transport is current by the time it is handled. A permanent
        /// code disconnects. Otherwise the session reconnects, the transport's
        /// handlers unhooked first (outside the lock): the server closes it after the push, and its
        /// Closed must not start a second reconnect nor its buffered frames raise events of a dead
        /// connection.
        /// </summary>
        private void HandleDisconnectPush(Disconnect disconnect)
        {
            var transport = t_dispatchingTransport;
            int epoch;
            lock (_stateChangeLock)
            {
                if (transport == null || !ReferenceEquals(_transport, transport)) return;
                epoch = _epoch;
            }

            int code = (int)disconnect.Code;
            _logger?.LogDebug($"HandleDisconnectPush - code: {code}, reason: '{disconnect.Reason}'");

            if (IsPermanentDisconnectCode(code))
            {
                _ = SetDisconnectedAsync(code, disconnect.Reason, epoch: epoch);
                return;
            }

            UnhookTransport(transport);
            _ = HandleTransportClosedAsync(transport, new TransportClosedEventArgs(code: code, reason: disconnect.Reason));
        }

        /// <summary>Server disconnect codes 3500-3999 and 4500-4999 end the client for good.</summary>
        private static bool IsPermanentDisconnectCode(int code) =>
            (code >= 3500 && code < 4000) || (code >= 4500 && code < 5000);

        private void OnTransportClosed(object? sender, TransportClosedEventArgs e)
        {
            // Defensive check: don't process events if client is disposed
            if (_disposed != 0) return;

            _logger?.LogDebug($"OnTransportClosed - code: {e.Code}, reason: '{e.Reason}'");
            if (!(sender is ITransport transport)) return;
            using var dispatch = new DispatchScope(transport);
            _ = HandleTransportClosedAsync(transport, e);
        }

        /// <summary>
        /// A close of the current transport. A code below 3000 (a WebSocket code, or none) is a transport
        /// close: the client reconnects with TransportClosed, except 1009 (message too big), which
        /// disconnects with MessageSizeLimit — as centrifuge-js. Server codes 3500–3999 and 4500–4999
        /// disconnect for good; other server codes reconnect with the server's code and reason. A server
        /// code proves the transport reaches the server (_transportWasOpen).
        /// </summary>
        private async Task HandleTransportClosedAsync(ITransport closedTransport, TransportClosedEventArgs e)
        {
            _logger?.LogDebug($"HandleTransportClosedAsync - state: {_state}, code: {e.Code}, reason: '{e.Reason}'");

            // Don't process if already disconnected
            int epoch;
            lock (_stateChangeLock)
            {
                epoch = _epoch;
                if (_state == CentrifugeClientState.Disconnected)
                {
                    _logger?.LogDebug("HandleTransportClosedAsync - skipping (already disconnected)");
                    return;
                }
                // Ignore close events from a transport that is no longer the current one —
                // e.g. a stale Closed event from an old connection that was queued on the
                // thread pool just before CleanupTransportAsync detached its handlers. Such
                // an event arriving after a successful reconnect would otherwise tear down
                // the healthy new connection and flip the client back to Connecting.
                if (!ReferenceEquals(closedTransport, _transport))
                {
                    _logger?.LogDebug("HandleTransportClosedAsync - skipping (stale transport close)");
                    return;
                }
                if (e.Code >= 3000) _transportWasOpen = true;
            }

            bool shouldReconnect = true;
            int code = e.Code ?? 0;
            string reason = e.Reason;
            if (code < 3000)
            {
                shouldReconnect = code != 1009;
                code = shouldReconnect ? CentrifugeConnectingCodes.TransportClosed : CentrifugeDisconnectedCodes.MessageSizeLimit;
                reason = shouldReconnect ? "transport closed" : "message size limit exceeded";
            }
            else if (IsPermanentDisconnectCode(code))
            {
                shouldReconnect = false;
            }
            _logger?.LogDebug($"HandleTransportClosedAsync - processing with code: {code}, reason: '{reason}'");

            if (!shouldReconnect)
            {
                // Permanent disconnect
                await SetDisconnectedAsync(code, reason, epoch: epoch).ConfigureAwait(false);
                return;
            }

            ReconnectSession(() => ReferenceEquals(closedTransport, _transport), code, reason,
                invalidateState: code == CentrifugeDisconnectedCodes.StateInvalidated,
                advanceUnopenedTransport: true);
        }

        /// <summary>
        /// Ends the current transport session and reconnects. The transition to Connecting, the
        /// session's detach (DetachTransportLocked) and the reconnect happen only while
        /// <paramref name="sessionIsCurrent"/> holds under the state lock: the outcome that ends a
        /// session belongs to it, not to a newer one. Subscriptions move to subscribing before the
        /// events; the client's events precede theirs (as in SetDisconnectedAsync), so a handler
        /// reacting to a subscription can't hide the client transition. Connecting is raised only on the
        /// transition (as centrifuge-js): a transport ending while already Connecting adds none. The
        /// transport is cleaned up after the events, without waiting for its close (see
        /// CleanupTransportAsync), and the reconnect is started without waiting for it either.
        /// </summary>
        /// <param name="sessionIsCurrent">Evaluated under _stateChangeLock.</param>
        /// <param name="code">Connecting code.</param>
        /// <param name="reason">Connecting reason.</param>
        /// <param name="invalidateState">State invalidated (a WebSocket close code or a Disconnect
        /// push): the next connect fetches a fresh token when GetToken can (a static token stays,
        /// as centrifuge-js), and every subscription's and server-side
        /// subscription's cached position is dropped before they move to subscribing — server-side
        /// ones reset to the sentinel epoch "_" the server can never match.</param>
        /// <param name="advanceUnopenedTransport">A transport that closed before it proved to reach the server
        /// (_transportWasOpen): fallback mode moves to the next endpoint, in the same critical section as
        /// the session check.</param>
        private void ReconnectSession(Func<bool> sessionIsCurrent, int code, string reason,
            bool invalidateState = false, bool advanceUnopenedTransport = false)
        {
            CentrifugeClientState prevState;
            ITransport? detached;
            CancellationTokenSource? write;
            int epoch;
            long endedGeneration;
            lock (_stateChangeLock)
            {
                if (_state == CentrifugeClientState.Disconnected || !sessionIsCurrent()) return;
                if (advanceUnopenedTransport) AdvanceUnprovenTransportLocked();
                _reconnectCts?.Cancel();
                prevState = SetState(CentrifugeClientState.Connecting);
                if (prevState == CentrifugeClientState.Connected) _serverSubscribingOwed.UnionWith(_serverSubscriptions.Keys);
                detached = DetachTransportLocked(out write);
                epoch = _epoch;
                endedGeneration = _connectionGeneration - 1;
                if (invalidateState)
                {
                    if (_options.GetToken != null)
                    {
                        _token = string.Empty;
                        _refreshRequired = true;
                    }
                    foreach (var entry in _serverSubscriptions)
                    {
                        _serverSubscriptions[entry.Key] = new ServerSubscription(0, "_", entry.Value.Recoverable);
                    }
                }
            }

            CancelWrite(write);
            var subscribing = MoveSubscriptionsToSubscribing(invalidateState, endedGeneration);

            if (prevState != CentrifugeClientState.Connecting)
                Raise(StateChanged, new CentrifugeStateEventArgs(prevState, CentrifugeClientState.Connecting), "stateChanged");
            if (prevState != CentrifugeClientState.Connecting && IsEpoch(epoch))
                Raise(Connecting, new CentrifugeConnectingEventArgs(code, reason), "connecting");

            foreach (var raise in subscribing) raise();
            RaiseServerSubscribing();
            ResweepIfConnected();

            _ = CleanupTransportAsync(detached, waitClose: false);
            _ = ScheduleReconnectAsync(epoch);
        }

        /// <summary>An error of a transport the session no longer uses (detached by a teardown) isn't the client's.</summary>
        private void OnTransportError(object? sender, Exception e)
        {
            // Defensive check: don't process events if client is disposed
            if (_disposed != 0 || !(sender is ITransport transport) || !IsDispatching(transport)) return;

            using var dispatch = new DispatchScope(transport);
            OnError("transport", e);
        }

        /// <summary>Subscribe timeout: reconnects the session <paramref name="connectionGeneration"/> the subscribe was sent on, if still current.</summary>
        internal void HandleSubscribeTimeout(long connectionGeneration) =>
            ReconnectSession(() => _connectionGeneration == connectionGeneration,
                CentrifugeConnectingCodes.SubscribeTimeout, "subscribe timeout");

        /// <summary>Unsubscribe error (as centrifuge-js): reconnects the session <paramref name="connectionGeneration"/> the unsubscribe was sent on, if still current.</summary>
        internal void HandleUnsubscribeError(long connectionGeneration) =>
            ReconnectSession(() => _connectionGeneration == connectionGeneration,
                CentrifugeConnectingCodes.UnsubscribeError, "unsubscribe error");

        /// <summary>
        /// A publication dispatched on this thread failed to decode: the stream of its transport is
        /// corrupt, and the client disconnects with BadProtocol (as centrifuge-js), if that transport
        /// is still current.
        /// </summary>
        internal Task HandleUndecodablePublicationAsync()
        {
            var transport = t_dispatchingTransport;
            int epoch;
            lock (_stateChangeLock)
            {
                if (transport == null || !ReferenceEquals(_transport, transport)) return Task.CompletedTask;
                epoch = _epoch;
            }
            return SetDisconnectedAsync(CentrifugeDisconnectedCodes.BadProtocol, "bad protocol", epoch: epoch);
        }

        /// <summary>
        /// Detaches the current transport together with the state of its session and ends the
        /// state epoch: disposes the ping and token refresh timers, fails the pending calls one by one
        /// (the receive loop may concurrently claim a reply; continuations run asynchronously), drops
        /// the queued commands, the channel compaction IDs and the emulation connect call and
        /// endpoint, and starts a fresh flush lock. Call under
        /// _stateChangeLock, in the critical section of the transition that ends the session,
        /// so the teardown applies to exactly that session and the outcomes of its connect
        /// attempt no longer match the epoch. Returns the transport for
        /// <see cref="CleanupTransportAsync"/>.
        /// </summary>
        private ITransport? DetachTransportLocked(out CancellationTokenSource? write)
        {
            _epoch++;
            return ReleaseSessionLocked(out write);
        }

        /// <summary>Cancels the write a released session left in progress (see ReleaseSessionLocked) and
        /// disposes its deadline — outside the locks: the cancellation may complete the write inline.</summary>
        private static void CancelWrite(CancellationTokenSource? write)
        {
            if (write == null) return;
            write.Cancel();
            write.Dispose();
        }

        /// <summary>
        /// Releases the current transport and the state of its session (see DetachTransportLocked)
        /// without ending the state epoch: a failed connect attempt is replaced within its Connecting
        /// state, and its commands and pending calls don't reach the next transport. The deadline of the
        /// write in progress is taken over (<paramref name="write"/>) for the caller to cancel with
        /// CancelWrite after the locks. Call under _stateChangeLock.
        /// </summary>
        private ITransport? ReleaseSessionLocked(out CancellationTokenSource? write)
        {
            var transport = _transport;
            _transport = null;
            _pendingConnectCall = null;
            _emulationEndpoint = null;
            _pingTimer?.Dispose();
            _pingTimer = null;
            ClearRefreshTimer();
            _serverPingInterval = 0;
            _sendPong = false;
            _reconnectAttemptsResetPending = false;
            _subscriptionsByPushId.Clear();

            var connClosedEx = ConnectionClosedError();
            foreach (var id in _pendingCalls.Keys)
            {
                if (_pendingCalls.TryRemove(id, out var pending)) FailObserved(pending, connClosedEx);
            }

            lock (_commandBatchLock)
            {
                foreach (var entry in _commandBatch) FailObserved(entry.Written, connClosedEx);
                write = _writingDeadline;
                _writingDeadline = null;
                _commandBatch.Clear();
                _commandBatchSize = 0;
                _commandBatchPending = false;
                _commandBatchTimer?.Dispose();
                _commandBatchTimer = null;
            }
            _flushLock = new SemaphoreSlim(1, 1);
            return transport;
        }

        /// <summary>
        /// Completes a teardown started by <see cref="DetachTransportLocked"/>: unhooks, closes and
        /// disposes the detached transport. The returned task covers the close only with
        /// <paramref name="waitClose"/>: a reconnect doesn't wait for it (the dead connection it
        /// replaces may hold the close back — a stuck write, a full send buffer), nor does a
        /// teardown inside that transport's own dispatch (e.g. a synchronous Dispose() in an event
        /// handler), where CloseAsync's wait for the receive loop would never end.
        /// </summary>
        private Task CleanupTransportAsync(ITransport? transport, bool waitClose)
        {
            if (transport == null) return Task.CompletedTask;
            UnhookTransport(transport);
            var close = CloseTransportAsync(transport);
            return waitClose ? close : Task.CompletedTask;
        }

        private static async Task CloseTransportAsync(ITransport transport)
        {
            try
            {
                await transport.CloseAsync().ConfigureAwait(false);
            }
            catch
            {
                // Ignore cleanup errors
            }
            transport.Dispose();
        }

        /// <summary>
        /// Schedules the next connect attempt of the Connecting state identified by
        /// <paramref name="epoch"/>, and after each attempt that fails to create or open a
        /// transport the one after it — a loop, so a long outage doesn't build a chain of pending
        /// attempts. No-op once that state was left (e.g. an event handler called Disconnect(),
        /// maybe followed by Connect()): a reconnect of a superseded attempt would replace the
        /// transport of the current one. Only the cancellation of this reconnect ends it: one
        /// raised inside the attempt (a GetToken timeout) is its failure.
        /// </summary>
        private async Task ScheduleReconnectAsync(int epoch)
        {
            while (true)
            {
                CancellationToken reconnectToken;
                int currentAttempts;
                lock (_stateChangeLock)
                {
                    if (_epoch != epoch) return;

                    var oldCts = _reconnectCts;
                    _reconnectCts = new CancellationTokenSource();
                    oldCts?.Cancel();
                    oldCts?.Dispose();
                    reconnectToken = _reconnectCts.Token;
                    currentAttempts = _reconnectAttempts++;
                }
                int delay = Utilities.CalculateBackoff(
                    currentAttempts,
                    _options.MinReconnectDelay,
                    _options.MaxReconnectDelay
                );

                try
                {
                    await Task.Delay(delay, reconnectToken).ConfigureAwait(false);
                    lock (_stateChangeLock)
                    {
                        if (_epoch != epoch) return;
                    }
                    await CreateTransportAsync(epoch).ConfigureAwait(false);
                    return;
                }
                catch (OperationCanceledException) when (reconnectToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    if (!await ReportConnectAttemptErrorAsync(ex, epoch).ConfigureAwait(false)) return;
                }
            }
        }

        /// <summary>
        /// Moves the client to Disconnected: the state changes under the lock, the events are raised
        /// outside it (a handler may call back into the SDK), and Disconnected only while no handler
        /// moved the client on. Client-side subscriptions stay registered and move to Subscribing
        /// before any handler runs, visibly once Disconnect() returns, to resubscribe on the next
        /// connect — as in the other Centrifugal SDKs, only Unsubscribe(), unauthorized errors,
        /// terminal server unsubscribe codes or disposal unsubscribe them.
        /// </summary>
        /// <param name="code">Disconnect code.</param>
        /// <param name="reason">Disconnect reason.</param>
        /// <param name="disposing">Disposal: subscriptions are unsubscribed by the caller instead.</param>
        /// <param name="epoch">When set, disconnect only if the client is still in that state
        /// epoch — the outcome of a connect attempt or session that is not superseded.</param>
        /// <param name="waitClose">Whether the returned task covers the transport close.</param>
        private async Task SetDisconnectedAsync(int code, string reason, bool disposing = false, int? epoch = null, bool waitClose = true)
        {
            var dispatching = t_dispatchingTransport;
            CentrifugeClientState prevState;
            ITransport? detached;
            CancellationTokenSource? write;
            int disconnectedEpoch;
            long endedGeneration;
            lock (_stateChangeLock)
            {
                if (_state == CentrifugeClientState.Disconnected || (epoch.HasValue && _epoch != epoch.Value))
                {
                    return;
                }

                prevState = SetState(CentrifugeClientState.Disconnected);
                endedGeneration = _connectionGeneration - 1;
                if (prevState == CentrifugeClientState.Connected) _serverSubscribingOwed.UnionWith(_serverSubscriptions.Keys);
                _readyPromises.RejectAll(new CentrifugeException(CentrifugeErrorCodes.ClientDisconnected, "client disconnected"));

                _transportWasOpen = false;
                try { _reconnectCts?.Cancel(); } catch (ObjectDisposedException) { }
                _reconnectCts?.Dispose();
                _reconnectCts = null;
                detached = DetachTransportLocked(out write);
                disconnectedEpoch = _epoch;
            }

            CancelWrite(write);
            var subscribing = disposing ? null : MoveSubscriptionsToSubscribing(invalidateState: false, endedGeneration);

            Raise(StateChanged, new CentrifugeStateEventArgs(prevState, CentrifugeClientState.Disconnected), "stateChanged");
            if (IsEpoch(disconnectedEpoch))
                Raise(Disconnected, new CentrifugeDisconnectedEventArgs(code, reason), "disconnected");

            if (subscribing != null)
            {
                foreach (var raise in subscribing) raise();
                RaiseServerSubscribing();
                ResweepIfConnected();
            }

            await CleanupTransportAsync(detached, waitClose: waitClose && !ReferenceEquals(dispatching, detached)).ConfigureAwait(false);
        }

        /// <summary>
        /// Subscriptions move to Subscribing outside the client lock: a Connect() on another thread
        /// may have connected and swept them just before, skipping them as still subscribed. A client
        /// that is Connected by now sweeps again, after their events; the sweep skips inflight ones.
        /// </summary>
        private void ResweepIfConnected()
        {
            bool connected;
            lock (_stateChangeLock) { connected = _state == CentrifugeClientState.Connected; }
            if (connected) ScheduleSubscribeBatch();
        }

        private CentrifugeClientState SetState(CentrifugeClientState newState)
        {
            var oldState = _state;
            _state = newState;
            if (oldState != newState) _epoch++;
            // Leaving Connected ends the current connection session: replies produced
            // by it that are still in flight through the processing pipeline must not
            // be applied anymore (see HandleSubscribeReply generation check). SetState
            // is always called under _stateChangeLock, before subscriptions are moved
            // to subscribing — so a reply observing the old generation is guaranteed
            // to have fully applied before the teardown touches subscription state.
            if (oldState == CentrifugeClientState.Connected && newState != CentrifugeClientState.Connected)
            {
                Interlocked.Increment(ref _connectionGeneration);
            }
            return oldState;
        }

        /// <summary>
        /// Identifies the current Connected session. Bumped whenever the client
        /// leaves Connected state. Captured by the subscribe pipeline before
        /// sending and compared when processing the reply, so a reply from a
        /// torn-down connection can't flip a subscription to Subscribed after
        /// the teardown already moved it to Subscribing.
        /// </summary>
        internal long ConnectionGeneration => Interlocked.Read(ref _connectionGeneration);

        /// <summary>
        /// The transport and generation of the current Connected session, or null when the
        /// client is not Connected: subscription commands are bound to a session.
        /// </summary>
        internal (ITransport Transport, long Generation)? ConnectedSession
        {
            get
            {
                lock (_stateChangeLock)
                {
                    return _state == CentrifugeClientState.Connected && _transport != null
                        ? (_transport, _connectionGeneration)
                        : ((ITransport, long)?)null;
                }
            }
        }

        private long _connectionGeneration;

        /// <summary>No ping within the interval: reconnects the Connected session of <paramref name="session"/>, if still current.</summary>
        internal void NoPing(ITransport session) =>
            ReconnectSession(() => _state == CentrifugeClientState.Connected && ReferenceEquals(_transport, session),
                CentrifugeConnectingCodes.NoPing, "no ping");

        /// <summary>The ping timer of the Connected session of <paramref name="session"/>: a callback that
        /// started before the session ended can't tear down the next one.</summary>
        private void StartPingTimer(uint pingInterval, ITransport session)
        {
            _pingTimer?.Dispose();

            var interval = Utilities.PingDeadlineMilliseconds(pingInterval, _options.MaxServerPingDelay);
            _pingTimer = new Timer(_ => NoPing(session), null, interval, System.Threading.Timeout.Infinite);
        }

        /// <summary>Restarts the no-ping deadline on a received message; no-op without server pings or outside Connected.</summary>
        private void ResetPingTimer()
        {
            Timer? timer;
            uint pingInterval;
            lock (_stateChangeLock)
            {
                if (_pingTimer == null || _serverPingInterval == 0 || _state != CentrifugeClientState.Connected)
                    return;
                timer = _pingTimer;
                pingInterval = _serverPingInterval;
            }
            var interval = Utilities.PingDeadlineMilliseconds(pingInterval, _options.MaxServerPingDelay);
            try { timer.Change(interval, System.Threading.Timeout.Infinite); }
            catch (ObjectDisposedException) { }
        }

        /// <summary>
        /// Answers a ping with a pong sent at once on the session the ping arrived on, while it is
        /// current, bypassing the command queue: behind queued commands it could outlast the server's
        /// pong timeout. A failed pong is left to the transport, as in centrifuge-js: a WebSocket
        /// closes on a failed send, a missed pong makes the server end the session.
        /// </summary>
        private void HandleServerPing()
        {
            ConfirmSessionSteady();
            ITransport transport;
            EmulationTarget? emulation;
            lock (_stateChangeLock)
            {
                if (!_sendPong || _transport == null || !ReferenceEquals(_transport, t_dispatchingTransport)) return;
                transport = _transport;
                emulation = transport.UsesEmulation ? new EmulationTarget(_session, _node, _emulationEndpoint!) : null;
            }

            _ = SendCommandsAsync(transport, emulation, new[] { new Command() }).ContinueWith(t =>
                {
                    var error = t.Exception;
                    _logger?.LogDebug($"Error sending pong: {error?.InnerException?.InnerException?.Message}");
                },
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);
        }

        /// <summary>Arms the token refresh of the current Connected session. Call under _stateChangeLock.</summary>
        private void ScheduleConnectionRefresh(uint ttl)
        {
            _refreshTimer?.Dispose();
            _refreshTimer = null;
            // ttl=0 means "no expiry given" — skip scheduling rather than busy-loop.
            if (ttl == 0) return;
            var delay = Utilities.TtlToMilliseconds(ttl);
            var epoch = _epoch;
            _refreshTimer = new Timer(_ => _ = RefreshConnectionTokenAsync(epoch), null, delay, System.Threading.Timeout.Infinite);
        }

        private void ClearRefreshTimer()
        {
            _refreshTimer?.Dispose();
            _refreshTimer = null;
        }

        /// <summary>Retries the connection token refresh of the Connected session <paramref name="epoch"/> with backoff.</summary>
        private void ScheduleConnectionRefreshRetry(int epoch)
        {
            lock (_stateChangeLock)
            {
                if (!IsCurrentSessionLocked(epoch)) return;
                var delay = Utilities.CalculateBackoff(_refreshAttempts++, _options.MinReconnectDelay, _options.MaxReconnectDelay);
                ClearRefreshTimer();
                _refreshTimer = new Timer(_ => _ = RefreshConnectionTokenAsync(epoch), null, delay, System.Threading.Timeout.Infinite);
            }
        }

        /// <summary>
        /// Refreshes the connection token of the Connected session <paramref name="epochSnapshot"/> (the
        /// one its timer was armed in: a callback of an ended session does nothing) before it expires.
        /// Without GetToken the expiring token can't be replaced: a configuration error, then
        /// Unauthorized (as centrifuge-js).
        /// </summary>
        internal async Task RefreshConnectionTokenAsync(int epochSnapshot)
        {
            lock (_stateChangeLock)
            {
                if (!IsCurrentSessionLocked(epochSnapshot)) return;
            }

            if (_options.GetToken == null)
            {
                OnError("configuration", new CentrifugeConfigurationException("token expired but no GetToken is set"));
                await SetDisconnectedAsync(CentrifugeDisconnectedCodes.Unauthorized, "unauthorized", epoch: epochSnapshot).ConfigureAwait(false);
                return;
            }

            string token;
            try
            {
                token = await _options.GetToken().ConfigureAwait(false);
            }
            catch (CentrifugeUnauthorizedException)
            {
                await SetDisconnectedAsync(CentrifugeDisconnectedCodes.Unauthorized, "unauthorized", epoch: epochSnapshot).ConfigureAwait(false);
                return;
            }
            catch (Exception ex)
            {
                if (!IsCurrentSession(epochSnapshot)) return;
                OnError("refreshToken", new CentrifugeException(CentrifugeErrorCodes.ClientRefreshToken, ex.Message, true, ex));
                ScheduleConnectionRefreshRetry(epochSnapshot);
                return;
            }
            if (string.IsNullOrEmpty(token))
            {
                await SetDisconnectedAsync(CentrifugeDisconnectedCodes.Unauthorized, "unauthorized", epoch: epochSnapshot).ConfigureAwait(false);
                return;
            }

            ITransport? session;
            lock (_stateChangeLock)
            {
                if (!IsCurrentSessionLocked(epochSnapshot)) return;
                _token = token;
                session = _transport;
            }

            try
            {
                var reply = await SendCommandAsync(new Command { Id = NextCommandId(), Refresh = new RefreshRequest { Token = token } },
                    session, null, CancellationToken.None).ConfigureAwait(false);
                if (reply.Error != null)
                {
                    HandleRefreshError(CentrifugeException.FromReply(reply.Error), epochSnapshot);
                    return;
                }

                HandleRefreshReply(reply.Refresh, epochSnapshot);
            }
            catch (Exception ex)
            {
                if (!IsCurrentSession(epochSnapshot)) return;
                OnError("refresh", ex);
                ScheduleConnectionRefreshRetry(epochSnapshot);
            }
        }

        /// <summary>Whether the client is still in the state epoch <paramref name="epoch"/>: a handler
        /// of a transition's first event may have moved it on, and the rest are not raised.</summary>
        private bool IsEpoch(int epoch)
        {
            lock (_stateChangeLock)
            {
                return _epoch == epoch;
            }
        }

        /// <summary>Whether the client is still in the Connected session identified by <paramref name="epoch"/>.</summary>
        private bool IsCurrentSession(int epoch)
        {
            lock (_stateChangeLock)
            {
                return IsCurrentSessionLocked(epoch);
            }
        }

        /// <summary>IsCurrentSession under _stateChangeLock.</summary>
        private bool IsCurrentSessionLocked(int epoch) => _state == CentrifugeClientState.Connected && _epoch == epoch;

        private void HandleRefreshReply(RefreshResult result, int epoch)
        {
            lock (_stateChangeLock)
            {
                if (!IsCurrentSessionLocked(epoch)) return;
                ClearRefreshTimer();
                _refreshAttempts = 0;
                if (result.Expires)
                {
                    ScheduleConnectionRefresh(result.Ttl);
                }
            }
        }

        /// <summary>An error of a refresh of a session that is gone changes nothing, not even an Error event.</summary>
        private void HandleRefreshError(CentrifugeException error, int epoch)
        {
            if (!IsCurrentSession(epoch)) return;
            if (error.Code < 100 || error.Temporary)
            {
                OnError("refresh", error);
                ScheduleConnectionRefreshRetry(epoch);
            }
            else
            {
                _ = SetDisconnectedAsync((int)error.Code, error.Message, epoch: epoch);
            }
        }


        /// <summary>
        /// Moves the client-side subscriptions of the sessions up to <paramref name="endedGeneration"/> to
        /// Subscribing before any handler runs (see <see cref="CentrifugeSubscription.MoveToSubscribing"/>)
        /// and returns the raises of their events. With <paramref name="invalidateState"/> each drops its
        /// cached state first.
        /// </summary>
        private List<Action> MoveSubscriptionsToSubscribing(bool invalidateState, long endedGeneration)
        {
            var raises = new List<Action>();
            foreach (var sub in _subscriptions.Values)
            {
                if (sub.MoveToSubscribing(CentrifugeSubscribingCodes.TransportClosed, "transport closed", invalidateState, endedGeneration) is { } raise)
                    raises.Add(raise);
            }
            return raises;
        }

        /// <summary>
        /// Raises the ServerSubscribing owed to the server-side subscriptions of an ended Connected
        /// session (transport closed, no ping, explicit disconnect) — each once, whichever teardown,
        /// nested in a handler or not, gets to it first.
        /// <para>
        /// Server-side subscriptions have no Subscription object of their own, so this is
        /// the only signal an application gets that they went down and will be
        /// re-established — or dropped — on the next connect. Without it the app observes
        /// ServerSubscribed twice in a row across a reconnect with nothing in between.
        /// Matches centrifuge-js, which emits `subscribing` for every entry of its server
        /// subscription registry from _clearConnectedState().
        /// </para>
        /// </summary>
        private void RaiseServerSubscribing()
        {
            foreach (var channel in _serverSubscriptions.Keys)
            {
                RaiseOwedServerSubscribing(channel);
            }
        }

        /// <summary>Raises the ServerSubscribing <paramref name="channel"/> is owed, if any.</summary>
        private void RaiseOwedServerSubscribing(string channel)
        {
            bool owed;
            lock (_stateChangeLock) { owed = _serverSubscribingOwed.Remove(channel); }
            if (owed) Raise(ServerSubscribing, new CentrifugeServerSubscribingEventArgs(channel), "serverSubscribing");
        }

        /// <summary>
        /// Reads the server-side subscriptions of a connect reply against the registry. Call
        /// under _stateChangeLock. An empty map is not an early exit: every known channel is then
        /// gone. Returns the channels that were not known before, the channels that are gone
        /// (removed from the registry as their ServerUnsubscribed is raised, so one the app was not
        /// told about is still gone on the next connect) and the registry entry of each channel as
        /// of the reply (none for a new one), for <see cref="RaiseServerSubscriptionEvents"/>: the
        /// registry records what the app received, so it changes only as the events are delivered.
        /// </summary>
        private void ApplyServerSubscriptionsLocked(Google.Protobuf.Collections.MapField<string, SubscribeResult> subs,
            out HashSet<string> newChannels, out List<string> removedChannels,
            out Dictionary<string, ServerSubscription?> kept)
        {
            newChannels = new HashSet<string>();
            removedChannels = new List<string>();
            kept = new Dictionary<string, ServerSubscription?>();

            foreach (var kvp in subs)
            {
                if (!_serverSubscriptions.TryGetValue(kvp.Key, out var previous)) newChannels.Add(kvp.Key);
                kept[kvp.Key] = previous;
            }

            foreach (var channel in _serverSubscriptions.Keys)
            {
                if (!subs.ContainsKey(channel)) removedChannels.Add(channel);
            }
        }

        /// <summary>
        /// Raises the events of the server-side subscriptions read by
        /// <see cref="ApplyServerSubscriptionsLocked"/>. Stops once the client left the
        /// Connected session identified by <paramref name="connectedEpoch"/> (e.g. a handler
        /// disconnected): the rest belongs to a session that is gone. With recovered publications
        /// the registry entry stays where recovery started and advances with each delivered one, so
        /// a delivery cut short resumes there; otherwise it takes the reported base right before
        /// ServerSubscribed is raised (a handler may reconnect at once), so a channel whose
        /// ServerSubscribed never reached the app reports its base again.
        /// </summary>
        private void RaiseServerSubscriptionEvents(Google.Protobuf.Collections.MapField<string, SubscribeResult> subs,
            HashSet<string> newChannels, List<string> removedChannels,
            Dictionary<string, ServerSubscription?> kept, int connectedEpoch)
        {
            foreach (var kvp in subs)
            {
                var channel = kvp.Key;
                var sub = kvp.Value;
                if (!IsCurrentSession(connectedEpoch)) return;
                if (!newChannels.Contains(channel))
                    RaiseOwedServerSubscribing(channel);
                else
                    Raise(ServerSubscribing, new CentrifugeServerSubscribingEventArgs(channel), "serverSubscribing");

                CentrifugeStreamPosition? streamPosition = null;
                if (sub.Positioned || sub.Recoverable)
                {
                    streamPosition = new CentrifugeStreamPosition(sub.Offset, sub.Epoch);
                }

                if (!IsCurrentSession(connectedEpoch)) return;
                var delivered = kept[channel];
                if (!(sub.Recovered && sub.Publications.Count > 0 && delivered?.Epoch == sub.Epoch))
                    AdoptServerSubscriptionBase(channel, ref delivered, sub.Offset, sub.Epoch, sub.Recoverable);
                if (ServerSubscribed != null)
                {
                    Raise(ServerSubscribed, new CentrifugeServerSubscribedEventArgs(
                        channel,
                        sub.WasRecovering,
                        sub.Recovered,
                        sub.Recoverable,
                        sub.Positioned,
                        streamPosition,
                        sub.Data.ToByteArray()
                    ), "serverSubscribed");
                }

                if (sub.Recovered)
                {
                    foreach (var pub in sub.Publications)
                    {
                        if (!IsCurrentSession(connectedEpoch)) return;
                        DeliverServerPublication(channel, delivered, pub);
                    }
                }
                AdvanceServerSubscription(channel, delivered, sub.Offset, sub.Epoch);
            }

            foreach (var channel in removedChannels)
            {
                bool owed;
                lock (_stateChangeLock)
                {
                    if (!IsCurrentSessionLocked(connectedEpoch)) return;
                    _serverSubscriptions.TryRemove(channel, out _);
                    owed = _serverSubscribingOwed.Remove(channel);
                }
                if (owed) Raise(ServerSubscribing, new CentrifugeServerSubscribingEventArgs(channel), "serverSubscribing");
                Raise(ServerUnsubscribed, new CentrifugeServerUnsubscribedEventArgs(channel), "serverUnsubscribed");
            }
        }

        /// <summary>See <see cref="EventDispatch.Raise{TArgs}"/>.</summary>
        private void Raise<TArgs>(EventHandler<TArgs>? handler, TArgs args, string type) =>
            EventDispatch.Raise(this, handler, args, type, Error, _logger);

        /// <summary>
        /// Replaces the registry entry of <paramref name="channel"/> as of the reply or push,
        /// <paramref name="delivered"/> (none for a new channel), with the reported base as
        /// ServerSubscribed is raised — unless a newer base replaced it meanwhile.
        /// </summary>
        private void AdoptServerSubscriptionBase(string channel, ref ServerSubscription? delivered,
            ulong offset, string epoch, bool recoverable)
        {
            lock (_stateChangeLock)
            {
                _serverSubscriptions.TryGetValue(channel, out var current);
                if (!ReferenceEquals(current?.Base, delivered?.Base)) return;
                delivered = _serverSubscriptions[channel] = new ServerSubscription(offset, epoch, recoverable);
            }
        }

        /// <summary>
        /// Advances the registry entry of <paramref name="channel"/> to <paramref name="offset"/> within
        /// the base of <paramref name="delivered"/>, the entry this delivery started from — unless a newer
        /// base replaced it meanwhile. A publication's epoch, when set, is the stream's (as centrifuge-js).
        /// </summary>
        private void AdvanceServerSubscription(string channel, ServerSubscription? delivered, ulong offset, string epoch)
        {
            lock (_stateChangeLock) AdvanceServerSubscriptionLocked(channel, delivered, offset, epoch);
        }

        private void AdvanceServerSubscriptionLocked(string channel, ServerSubscription? delivered, ulong offset, string epoch)
        {
            if (delivered != null && _serverSubscriptions.TryGetValue(channel, out var current)
                && ReferenceEquals(current.Base, delivered.Base) && current.Past(offset, epoch) is { } next)
                _serverSubscriptions[channel] = next;
        }

        /// <summary>Raises a server-side publication and advances its entry within the base of
        /// <paramref name="delivered"/>; a connect its handler starts recovers past it (as centrifuge-js
        /// _pendingServerSubOffsets). Event args are built only for a subscriber.</summary>
        private void DeliverServerPublication(string channel, ServerSubscription? delivered, Publication pub)
        {
            if (pub.Offset == 0 || delivered == null)
            {
                if (Publication != null) Raise(Publication, CreatePublicationArgs(channel, pub), "publication");
                return;
            }

            long seq;
            lock (_stateChangeLock)
            {
                seq = ++_serverDeliverySeq;
                _deliveringServerPublication = (seq, channel, delivered.Base, pub.Offset, pub.Epoch);
            }
            if (Publication != null) Raise(Publication, CreatePublicationArgs(channel, pub), "publication");
            lock (_stateChangeLock)
            {
                if (_deliveringServerPublication?.Seq == seq) _deliveringServerPublication = null;
                AdvanceServerSubscriptionLocked(channel, delivered, pub.Offset, pub.Epoch);
            }
        }

        /// <summary>
        /// A Subscribe push. As for the channels of a connect reply, the registry takes the base
        /// right before ServerSubscribed, and the events stop once a handler moved the client out
        /// of this Connected session: a channel whose ServerSubscribed never reached the app is new
        /// again on the next connect. The push carries no recovery: WasRecovering and Recovered are
        /// false (as centrifuge-js).
        /// </summary>
        private void HandleServerSubscribe(string channel, Subscribe sub)
        {
            int connectedEpoch;
            ServerSubscription? delivered;
            lock (_stateChangeLock)
            {
                connectedEpoch = _epoch;
                _serverSubscriptions.TryGetValue(channel, out delivered);
            }

            CentrifugeStreamPosition? streamPosition = null;
            if (sub.Positioned || sub.Recoverable)
            {
                streamPosition = new CentrifugeStreamPosition(sub.Offset, sub.Epoch);
            }

            if (delivered == null)
            {
                Raise(ServerSubscribing, new CentrifugeServerSubscribingEventArgs(channel), "serverSubscribing");
            }

            if (!IsCurrentSession(connectedEpoch)) return;
            AdoptServerSubscriptionBase(channel, ref delivered, sub.Offset, sub.Epoch, sub.Recoverable);
            if (ServerSubscribed != null)
            {
                Raise(ServerSubscribed, new CentrifugeServerSubscribedEventArgs(
                    channel,
                    false,
                    false,
                    sub.Recoverable,
                    sub.Positioned,
                    streamPosition,
                    sub.Data.ToByteArray()
                ), "serverSubscribed");
            }
        }

        /// <summary>An unsubscribe push no Subscribed client-side subscription takes ends the server-side
        /// subscription of the channel, if any: an inactive client-side object doesn't shadow it.</summary>
        private void HandleServerUnsubscribe(string channel, Unsubscribe unsubscribe)
        {
            if (_subscriptions.TryGetValue(channel, out var clientSub)
                && clientSub.HandleServerUnsubscribe((int)unsubscribe.Code, unsubscribe.Reason))
                return;

            bool removed;
            bool owed;
            lock (_stateChangeLock)
            {
                removed = _serverSubscriptions.TryRemove(channel, out _);
                owed = _serverSubscribingOwed.Remove(channel);
            }
            if (owed) Raise(ServerSubscribing, new CentrifugeServerSubscribingEventArgs(channel), "serverSubscribing");
            if (removed)
                Raise(ServerUnsubscribed, new CentrifugeServerUnsubscribedEventArgs(channel), "serverUnsubscribed");
        }

        private void OnError(string type, Exception exception) =>
            EventDispatch.RaiseError(this, Error, type, exception, _logger);

        internal ILogger? Logger => _logger;

        internal uint NextCommandId()
        {
            // The protocol uses uint32 ids; id == 0 is reserved for push messages
            // (no reply), so skip it on wrap-around to avoid colliding with pushes.
            uint id;
            do
            {
                id = (uint)Interlocked.Increment(ref _commandId);
            } while (id == 0);
            return id;
        }

        internal static CentrifugePublicationEventArgs CreatePublicationArgs(string channel, Publication pub)
        {
            return CreatePublicationArgs(channel, pub, pub.Data.ToByteArray());
        }

        internal static CentrifugePublicationEventArgs CreatePublicationArgs(string channel, Publication pub, byte[] data)
        {
            CentrifugeClientInfo? info = pub.Info != null ? CentrifugeClientInfo.FromProtocol(pub.Info) : null;

            var tags = pub.Tags.Count > 0
                ? new Dictionary<string, string>(pub.Tags)
                : null;

            return new CentrifugePublicationEventArgs(
                channel,
                data,
                info,
                pub.Offset > 0 ? pub.Offset : null,
                tags
            );
        }

        /// <summary>
        /// Asynchronously disposes the client, ensuring disconnect completes before releasing resources.
        /// This is the recommended way to dispose the client. Called from a handler on the receive
        /// thread, it doesn't wait for that connection to close: closing is started and completes
        /// after the handler returns.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (System.Threading.Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;
            await DisposeCoreAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Synchronously disposes the client. This blocks until disconnect completes, except from a
        /// handler on the receive thread and with browser transports (JS interop, which a blocking wait
        /// would deadlock): there closing the connection is started without waiting.
        /// Consider using DisposeAsync() instead for better async/await support.
        /// </summary>
        public void Dispose()
        {
            if (System.Threading.Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;
#if NET6_0_OR_GREATER
            var waitClose = _jsRuntime == null;
#else
            var waitClose = true;
#endif
            DisposeCoreAsync(waitClose).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Disconnects, then unsubscribes every subscription with ClientClosed whatever state the
        /// client was in — rejecting their pending ReadyAsync — and releases the resources. The
        /// subscriptions are a snapshot: a handler may dispose the client reentrantly. Without
        /// <paramref name="waitClose"/> it completes synchronously, the transport closing on its own.
        /// </summary>
        private async Task DisposeCoreAsync(bool waitClose = true)
        {
            try
            {
                await SetDisconnectedAsync(CentrifugeDisconnectedCodes.DisconnectCalled, "disconnect called", disposing: true, waitClose: waitClose).ConfigureAwait(false);
            }
            catch
            {
                // Suppress exceptions during disposal - we're shutting down anyway
            }

            foreach (var sub in _subscriptions.Values)
            {
                await sub.SetUnsubscribedAsync(CentrifugeUnsubscribedCodes.ClientClosed, "client closed", sendUnsubscribe: false).ConfigureAwait(false);
                sub.Dispose();
            }
            _subscriptions.Clear();
        }
    }
}
