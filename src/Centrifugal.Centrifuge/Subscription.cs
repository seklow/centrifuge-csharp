using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Centrifugal.Centrifuge.Protocol;
using Google.Protobuf;

namespace Centrifugal.Centrifuge
{
    /// <summary>
    /// Represents a subscription to a channel.
    /// </summary>
    public class CentrifugeSubscription : IDisposable
    {
        private readonly CentrifugeClient _client;
        private readonly CentrifugeSubscriptionOptions _options;
        /// <summary>The subscription token, data and tags filter: taken from the options at construction
        /// and changed by the setters and token refreshes — never written back into the options, which
        /// the app may share between subscriptions.</summary>
        private string? _token;
        private ReadOnlyMemory<byte> _data;
        private CentrifugeFilterNode? _tagsFilter;
        private readonly object _stateChangeLock = new object();
        private readonly object _deltaLock = new object();
        private readonly ReadyPromises _readyPromises = new();

        private volatile CentrifugeSubscriptionState _state = CentrifugeSubscriptionState.Unsubscribed;
        private int _resubscribeAttempts;
        private CancellationTokenSource? _resubscribeCts;
        private CentrifugeStreamPosition? _streamPosition;
        /// <summary>Bumped by each new base of <see cref="_streamPosition"/> (GetState, reset, invalidation,
        /// subscribe reply): a delivery advances the position only within its base.</summary>
        private long _positionBase;
        /// <summary>The publication whose handler runs, in its base (Deliver); Seq names the delivery that
        /// owns it — receive loops of consecutive sessions may overlap.</summary>
        private (long Seq, long Base, ulong Offset, string Epoch)? _delivering;
        private long _deliverySeq;
        // Numeric channel ID assigned by the server when channel compaction is
        // negotiated. Pushes then carry this ID instead of the channel name.
        private long _pushChannelId;
        private bool _deltaNegotiated;
        private byte[]? _prevValue;
        private Timer? _refreshTimer;
        private int _refreshAttempts;
        private bool _refreshRequired;
        /// <summary>The subscribe attempt in flight (0 — none): only that attempt releases it, so a
        /// superseded one finishing late doesn't release a newer one.</summary>
        private long _inflightAttempt;
        private long _lastAttempt;
        /// <summary>Connection generations of the session the Subscribed state and the armed resubscribe
        /// backoff belong to: a teardown touches only those of the session it ended (MoveToSubscribing).</summary>
        private long _subscribedGeneration;
        private long _resubscribeGeneration;
        private int _disposed;
        private int _epoch;

        /// <summary>
        /// Gets the channel name.
        /// </summary>
        public string Channel { get; }

        /// <summary>
        /// Gets the current subscription state.
        /// </summary>
        public CentrifugeSubscriptionState State => _state;

        /// <summary>
        /// Test hook: whether a resubscribe retry is currently armed (scheduled and not
        /// cancelled). Lets integration tests synchronize with the retry pipeline through
        /// state polling instead of wall-clock sleeps.
        /// </summary>
        internal bool HasPendingResubscribe
        {
            get
            {
                lock (_stateChangeLock)
                {
                    return ResubscribePendingLocked;
                }
            }
        }

        /// <summary>A resubscribe backoff is being waited. Call under _stateChangeLock.</summary>
        private bool ResubscribePendingLocked => _resubscribeCts is { IsCancellationRequested: false };

        /// <summary>
        /// Test hook: the current state epoch, which identifies a subscribe attempt (see
        /// <see cref="SendSubscribeIfNeededAsync"/>).
        /// </summary>
        internal int Epoch
        {
            get
            {
                lock (_stateChangeLock)
                {
                    return _epoch;
                }
            }
        }

        /// <summary>
        /// Event raised when subscription state changes.
        /// </summary>
        public event EventHandler<CentrifugeSubscriptionStateEventArgs>? StateChanged;

        /// <summary>
        /// Event raised when subscription is subscribing.
        /// </summary>
        public event EventHandler<CentrifugeSubscribingEventArgs>? Subscribing;

        /// <summary>
        /// Event raised when subscription is subscribed.
        /// </summary>
        public event EventHandler<CentrifugeSubscribedEventArgs>? Subscribed;

        /// <summary>
        /// Event raised when subscription is unsubscribed.
        /// </summary>
        public event EventHandler<CentrifugeUnsubscribedEventArgs>? Unsubscribed;

        /// <summary>
        /// Event raised when a publication is received.
        /// </summary>
        public event EventHandler<CentrifugePublicationEventArgs>? Publication;

        /// <summary>
        /// Event raised when a join event is received.
        /// </summary>
        public event EventHandler<CentrifugeJoinEventArgs>? Join;

        /// <summary>
        /// Event raised when a leave event is received.
        /// </summary>
        public event EventHandler<CentrifugeLeaveEventArgs>? Leave;

        /// <summary>
        /// Event raised when an error occurs.
        /// </summary>
        public event EventHandler<CentrifugeErrorEventArgs>? Error;

        internal CentrifugeSubscription(CentrifugeClient client, string channel, CentrifugeSubscriptionOptions? options)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            Channel = channel ?? throw new ArgumentNullException(nameof(channel));
            _options = options?.Clone() ?? new CentrifugeSubscriptionOptions();
            _options.Validate();
            _token = _options.Token;
            _data = _options.Data.IsEmpty ? default : _options.Data.ToArray();
            _tagsFilter = _options.TagsFilter;

            if (_options.Since != null)
            {
                _streamPosition = _options.Since;
            }
        }

        /// <summary>
        /// Subscribes to the channel. This method returns immediately and starts the subscription process in the background.
        /// Use ReadyAsync() to wait for the subscription to be established, or use the Subscribed event.
        /// The disposal check and the transition are one critical section: a concurrent Dispose() either
        /// makes this throw or unsubscribes the Subscribing subscription.
        /// </summary>
        public void Subscribe()
        {
            CentrifugeSubscriptionState prevState;
            int epoch;
            lock (_stateChangeLock)
            {
                ThrowIfDisposed();
                if (_state != CentrifugeSubscriptionState.Unsubscribed)
                {
                    return;
                }
                _resubscribeAttempts = 0;
                prevState = SetState(CentrifugeSubscriptionState.Subscribing);
                epoch = _epoch;
            }
            RaiseSubscribing(prevState, epoch, CentrifugeSubscribingCodes.SubscribeCalled, "subscribe called");
            _ = Task.Run(SendSubscribeIfNeededAsync);
        }

        private void ThrowIfDisposed()
        {
            if (System.Threading.Interlocked.CompareExchange(ref _disposed, 0, 0) != 0)
                throw new ObjectDisposedException(nameof(CentrifugeSubscription));
        }

        /// <summary>
        /// Unsubscribes from the channel. This method returns immediately and starts the unsubscription process in the background.
        /// </summary>
        public void Unsubscribe()
        {
            _ = SetUnsubscribedAsync(CentrifugeUnsubscribedCodes.UnsubscribeCalled, "unsubscribe called", sendUnsubscribe: true);
        }

        /// <summary>
        /// Returns a Task that completes when the subscription is established.
        /// If already subscribed, the Task completes immediately.
        /// If unsubscribed, the Task is rejected.
        /// </summary>
        /// <param name="timeout">Optional timeout.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task that completes when subscribed.</returns>
        /// <exception cref="ArgumentOutOfRangeException">The timeout is negative (other than infinite) or too large for a timer.</exception>
        public Task ReadyAsync(TimeSpan? timeout = null, CancellationToken cancellationToken = default)
        {
            Utilities.ValidateWaitTimeout(timeout, nameof(timeout));
            lock (_stateChangeLock)
            {
                switch (_state)
                {
                    case CentrifugeSubscriptionState.Unsubscribed:
                        return Task.FromException(new CentrifugeException(CentrifugeErrorCodes.SubscriptionUnsubscribed, "subscription unsubscribed"));

                    case CentrifugeSubscriptionState.Subscribed:
                        return Task.CompletedTask;
                }

                return _readyPromises.Add(timeout, cancellationToken);
            }
        }


        /// <summary>
        /// Sets the subscription data. This will be used for all subsequent subscription attempts.
        /// The data is copied internally to prevent external modifications.
        /// </summary>
        /// <param name="data">New subscription data.</param>
        public void SetData(ReadOnlyMemory<byte> data)
        {
            lock (_stateChangeLock)
            {
                _data = data.IsEmpty ? default : data.ToArray();
            }
        }

        /// <summary>
        /// Sets server-side publication filter based on publication tags.
        /// This allows filtering publications on the server side before they are sent to the client.
        /// The filter is applied on the next subscription/resubscription attempt.
        /// Cannot be used together with delta compression.
        /// </summary>
        /// <param name="tagsFilter">The filter expression, or null to remove filtering.</param>
        /// <exception cref="InvalidOperationException">Thrown when trying to set tags filter while delta compression is enabled.</exception>
        /// <example>
        /// // Simple equality filter
        /// sub.SetTagsFilter(CentrifugeFilterNodeBuilder.Eq("ticker", "BTC"));
        ///
        /// // Complex filter with logical operators
        /// sub.SetTagsFilter(
        ///     CentrifugeFilterNodeBuilder.And(
        ///         CentrifugeFilterNodeBuilder.Eq("ticker", "BTC"),
        ///         CentrifugeFilterNodeBuilder.Gt("price", "50000")
        ///     )
        /// );
        ///
        /// // Filter with IN operator
        /// sub.SetTagsFilter(CentrifugeFilterNodeBuilder.In("ticker", "BTC", "ETH", "SOL"));
        /// </example>
        public void SetTagsFilter(CentrifugeFilterNode? tagsFilter)
        {
            lock (_stateChangeLock)
            {
                if (tagsFilter != null && !string.IsNullOrEmpty(_options.Delta))
                {
                    throw new InvalidOperationException("Cannot use delta and TagsFilter together");
                }
                _tagsFilter = tagsFilter;
            }
        }

        /// <summary>
        /// Publishes data to the channel.
        /// Automatically waits for the subscription to be established before publishing.
        /// </summary>
        /// <param name="data">Data to publish.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public async Task PublishAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            // Wait for subscription to be ready with default timeout
            await ReadyAsync(_client.Timeout, cancellationToken).ConfigureAwait(false);

            var cmd = new Command
            {
                Id = _client.NextCommandId(),
                Publish = new PublishRequest
                {
                    Channel = Channel,
                    Data = ByteString.CopyFrom(data.Span)
                }
            };

            var reply = await _client.SendCommandAsync(cmd, cancellationToken).ConfigureAwait(false);

            if (reply.Error != null)
            {
                throw CentrifugeException.FromReply(reply.Error);
            }
        }

        /// <summary>
        /// Gets the channel history.
        /// Automatically waits for the subscription to be established before fetching history.
        /// </summary>
        /// <param name="options">History options.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>History result.</returns>
        public async Task<CentrifugeHistoryResult> HistoryAsync(CentrifugeHistoryOptions? options = null, CancellationToken cancellationToken = default)
        {
            // Wait for subscription to be ready with default timeout
            await ReadyAsync(_client.Timeout, cancellationToken).ConfigureAwait(false);

            var request = new HistoryRequest
            {
                Channel = Channel
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
                Id = _client.NextCommandId(),
                History = request
            };

            var reply = await _client.SendCommandAsync(cmd, cancellationToken).ConfigureAwait(false);

            if (reply.Error != null)
            {
                throw CentrifugeException.FromReply(reply.Error);
            }

            var publications = new List<CentrifugePublicationEventArgs>();
            foreach (var pub in reply.History.Publications)
            {
                publications.Add(CentrifugeClient.CreatePublicationArgs(Channel, pub));
            }

            return new CentrifugeHistoryResult(
                publications.ToArray(),
                reply.History.Epoch,
                reply.History.Offset
            );
        }

        /// <summary>
        /// Gets the channel presence.
        /// Automatically waits for the subscription to be established before fetching presence.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Presence result.</returns>
        public async Task<CentrifugePresenceResult> PresenceAsync(CancellationToken cancellationToken = default)
        {
            // Wait for subscription to be ready with default timeout
            await ReadyAsync(_client.Timeout, cancellationToken).ConfigureAwait(false);

            var cmd = new Command
            {
                Id = _client.NextCommandId(),
                Presence = new PresenceRequest
                {
                    Channel = Channel
                }
            };

            var reply = await _client.SendCommandAsync(cmd, cancellationToken).ConfigureAwait(false);

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
        /// Gets the channel presence stats.
        /// Automatically waits for the subscription to be established before fetching presence stats.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Presence stats result.</returns>
        public async Task<CentrifugePresenceStatsResult> PresenceStatsAsync(CancellationToken cancellationToken = default)
        {
            // Wait for subscription to be ready with default timeout
            await ReadyAsync(_client.Timeout, cancellationToken).ConfigureAwait(false);

            var cmd = new Command
            {
                Id = _client.NextCommandId(),
                PresenceStats = new PresenceStatsRequest
                {
                    Channel = Channel
                }
            };

            var reply = await _client.SendCommandAsync(cmd, cancellationToken).ConfigureAwait(false);

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
        /// A server unsubscribe push. It applies only to a Subscribed subscription (as centrifuge-js):
        /// one sent for a previous subscription must not end or reset a newer attempt. A code below
        /// 2500 unsubscribes; others resubscribe (see <see cref="Resubscribe"/>). Returns false when not
        /// Subscribed: the push isn't this subscription's.
        /// </summary>
        internal bool HandleServerUnsubscribe(int code, string reason)
        {
            int epoch;
            lock (_stateChangeLock)
            {
                if (_state != CentrifugeSubscriptionState.Subscribed) return false;
                epoch = _epoch;
            }

            if (code >= 2500)
                Resubscribe(code, reason);
            else
                _ = SetUnsubscribedAsync(code, reason, sendUnsubscribe: false, epoch);
            return true;
        }

        /// <summary>
        /// A temporary server unsubscribe of a Subscribed subscription: moves it to Subscribing with
        /// the server's code and reason (as centrifuge-js), StateInvalidated dropping the cached state
        /// in the same critical section, and schedules the subscribe off the receive loop, which the
        /// app's GetToken/GetState must not hold.
        /// </summary>
        internal void Resubscribe(int code, string reason)
        {
            CentrifugeSubscriptionState prevState;
            int epoch;
            lock (_stateChangeLock)
            {
                if (_state != CentrifugeSubscriptionState.Subscribed) return;
                if (code == CentrifugeUnsubscribedCodes.StateInvalidated) InvalidateState();
                prevState = SetState(CentrifugeSubscriptionState.Subscribing);
                epoch = _epoch;
            }

            RaiseSubscribing(prevState, epoch, code, reason);
            _ = Task.Run(SendSubscribeIfNeededAsync);
        }

        /// <summary>
        /// Moves the subscription to Subscribing when the client connection is lost, without raising
        /// its events: the client moves every subscription before any handler runs, so a handler that
        /// reconnects finds them all Subscribing. The teardown ended the sessions up to connection
        /// generation <paramref name="endedGeneration"/>: a subscription Subscribed on a newer one is
        /// left alone (and its state isn't invalidated), as is a backoff armed on one — a teardown
        /// finishing after a concurrent Connect doesn't touch what the new session owns, and still
        /// moves what its own session left Subscribed. Returns the raise of the events (null — no
        /// transition), which skips them once the subscription moved on.
        /// </summary>
        internal Action? MoveToSubscribing(int code, string reason, bool invalidateState, long endedGeneration)
        {
            CentrifugeSubscriptionState prevState;
            int epoch;
            lock (_stateChangeLock)
            {
                if (_state == CentrifugeSubscriptionState.Subscribed && _subscribedGeneration > endedGeneration) return null;
                if (invalidateState) InvalidateState();
                if (_state == CentrifugeSubscriptionState.Unsubscribed) return null;

                if (_resubscribeGeneration <= endedGeneration) _resubscribeCts?.Cancel();
                if (_state == CentrifugeSubscriptionState.Subscribing) return null;

                prevState = SetState(CentrifugeSubscriptionState.Subscribing);
                epoch = _epoch;
            }
            return () =>
            {
                if (IsEpoch(epoch)) RaiseSubscribing(prevState, epoch, code, reason);
            };
        }

        /// <summary>
        /// Raises the events of a transition to Subscribing entered in state epoch
        /// <paramref name="epoch"/>: Subscribing only while the StateChanged handler didn't move the
        /// subscription on.
        /// </summary>
        private void RaiseSubscribing(CentrifugeSubscriptionState prevState, int epoch, int code, string reason)
        {
            Raise(StateChanged, new CentrifugeSubscriptionStateEventArgs(prevState, CentrifugeSubscriptionState.Subscribing), "stateChanged");
            if (IsEpoch(epoch))
                Raise(Subscribing, new CentrifugeSubscribingEventArgs(code, reason), "subscribing");
        }

        /// <summary>
        /// Runs subscribe attempts while Subscribing — a loop, so a long outage doesn't build a
        /// chain of pending attempts. An attempt that ended without effect is followed up: one
        /// superseded by a newer attempt or by the end of its connection session hands over at
        /// once (the subscribe of the current one, or the sweep of the new session, was skipped
        /// while it was inflight); otherwise the next one follows a backoff. The backoff wait belongs
        /// to the loop: a sweep skips the subscription until it ends (as centrifuge-js).
        /// </summary>
        internal async Task SendSubscribeIfNeededAsync()
        {
            while (await RunSubscribeAttemptAsync().ConfigureAwait(false) is { } attempt)
            {
                if (!await DelayResubscribeAsync(attempt.Epoch, attempt.ConnectionGeneration).ConfigureAwait(false)) return;
            }
        }

        /// <summary>
        /// Runs a subscribe attempt. The state epoch identifies the attempt: it changes on every
        /// transition, so the attempt is superseded once the subscription leaves Subscribing. The
        /// command is queued under the state lock, bound to the Connected session: an unsubscribe is
        /// queued in the critical section of its transition (see SetUnsubscribedAsync), so the
        /// server gets both in the order of the transitions. The reply is applied on the receive
        /// loop (<see cref="HandleSubscribeReply"/>). An outcome of an attempt that is no longer
        /// current (superseded, or its session torn down) changes nothing but the follow-up.
        /// Returns the attempt to follow up, or null when there is nothing to retry or another attempt
        /// is inflight or waits its backoff.
        /// <para>
        /// The inflight mark is released by the reply on the receive loop, otherwise before any handler
        /// runs, so a re-subscribe from a handler isn't blocked. An unrecoverable position with
        /// GetState resets the position without an Error (as other SDKs), so the next attempt
        /// reloads state via GetState. A permanent error unsubscribes only if the attempt is still
        /// current after the Error handler. The attempt's session is read before the client state:
        /// one torn down meanwhile makes the attempt superseded, and the command is sent only in it. A
        /// token expired (109) is handled on the receive loop (MarkTokenExpired).
        /// </para>
        /// </summary>
        private async Task<(int Epoch, long ConnectionGeneration)?> RunSubscribeAttemptAsync()
        {
            long connectionGeneration = _client.ConnectionGeneration;
            if (_client.State != CentrifugeClientState.Connected)
            {
                return null;
            }

            int epoch;
            long attempt;
            lock (_stateChangeLock)
            {
                if (_inflightAttempt != 0 || _state != CentrifugeSubscriptionState.Subscribing || ResubscribePendingLocked)
                {
                    return null;
                }

                attempt = _inflightAttempt = ++_lastAttempt;
                epoch = _epoch;
            }

            try
            {
                var cmd = await BuildSubscribeCommandAsync(epoch, connectionGeneration).ConfigureAwait(false);
                Task<Reply>? send = null;
                bool applied = false;
                lock (_stateChangeLock)
                {
                    var connected = cmd != null && _epoch == epoch ? _client.ConnectedSession : null;
                    if (connected is { } session && session.Generation == connectionGeneration)
                    {
                        var generation = connectionGeneration;
                        send = _client.SendCommandAsync(cmd!, session.Transport, r =>
                        {
                            if (r.Error == null)
                                applied = HandleSubscribeReply(r.Subscribe ?? throw new System.IO.InvalidDataException("subscribe reply without result"), epoch, generation, attempt);
                            else if (r.Error.Code == 109)
                                MarkTokenExpired(epoch, generation);
                        }, CancellationToken.None);
                    }
                    else
                    {
                        ReleaseInflightLocked(attempt);
                    }
                }

                if (send != null)
                {
                    var reply = await send.ConfigureAwait(false);
                    if (reply.Error != null)
                    {
                        throw CentrifugeException.FromReply(reply.Error);
                    }

                    if (applied) return null;
                }

                return (epoch, connectionGeneration);
            }
            catch (CentrifugeTimeoutException)
            {
                if (!ReleaseAttempt(attempt, epoch, connectionGeneration)) return (epoch, connectionGeneration);
                OnError("subscribe", new CentrifugeException(CentrifugeErrorCodes.Timeout, "subscribe timeout", true));
                _client.HandleSubscribeTimeout(connectionGeneration);
                return null;
            }
            catch (CentrifugeGetStateException ex)
            {
                if (ReleaseAttempt(attempt, epoch, connectionGeneration)) OnError("getState", ex);
                return (epoch, connectionGeneration);
            }
            catch (CentrifugeException ex)
            {
                bool resetPosition = ex.Code == CentrifugeErrorCodes.UnrecoverablePosition && _options.GetState != null;
                bool current;
                lock (_stateChangeLock)
                {
                    ReleaseInflightLocked(attempt);
                    current = IsCurrentAttempt(epoch, connectionGeneration);
                    if (current && resetPosition)
                    {
                        _streamPosition = null;
                        _positionBase++;
                        lock (_deltaLock) { _prevValue = null; }
                    }
                }
                if (current && !resetPosition)
                    OnError(ex.Code == CentrifugeErrorCodes.SubscriptionSubscribeToken ? "subscribeToken" : "subscribe", ex);
                if (current && !resetPosition && ex.Code >= 100 && ex.Code != 109 && !ex.Temporary)
                {
                    await SetUnsubscribedAsync(ex.Code, ex.Message, sendUnsubscribe: false, epoch, connectionGeneration).ConfigureAwait(false);
                }
                return (epoch, connectionGeneration);
            }
            catch (Exception ex)
            {
                if (ReleaseAttempt(attempt, epoch, connectionGeneration)) OnError("subscribe", ex);
                return (epoch, connectionGeneration);
            }
        }

        /// <summary>
        /// Error 109 (token expired) of the attempt (<paramref name="epoch"/>, <paramref name="generation"/>),
        /// on the receive loop while the attempt is still current — a Disconnect behind the reply in the
        /// frame would end it: the token is dropped and refreshed by the next attempt; without GetToken
        /// the next subscribe goes without one (as centrifuge-js).
        /// </summary>
        private void MarkTokenExpired(int epoch, long generation)
        {
            lock (_stateChangeLock)
            {
                if (!IsCurrentAttempt(epoch, generation)) return;
                _token = string.Empty;
                _refreshRequired = true;
            }
        }

        /// <summary>Releases the inflight attempt; returns whether it is still current.</summary>
        private bool ReleaseAttempt(long attempt, int epoch, long connectionGeneration)
        {
            lock (_stateChangeLock)
            {
                ReleaseInflightLocked(attempt);
                return IsCurrentAttempt(epoch, connectionGeneration);
            }
        }

        /// <summary>Releases the inflight mark if <paramref name="attempt"/> still holds it. Call under _stateChangeLock.</summary>
        private void ReleaseInflightLocked(long attempt)
        {
            if (_inflightAttempt == attempt) _inflightAttempt = 0;
        }

        /// <summary>
        /// Builds the subscribe command of the attempt identified by <paramref name="epoch"/> and
        /// <paramref name="connectionGeneration"/>. Returns null when the attempt was superseded —
        /// before GetState/GetToken are called or while they are awaited: a result obtained before a
        /// teardown (which may have invalidated the state) is not taken. A null or empty token is
        /// Unauthorized (as centrifuge-js); its unsubscribe sends nothing: the attempt hasn't sent its
        /// subscribe. A GetToken failure other than Unauthorized is a temporary SubscriptionSubscribeToken
        /// error: retried with backoff, never taken for a subscribe reply.
        /// </summary>
        private async Task<Command?> BuildSubscribeCommandAsync(int epoch, long connectionGeneration)
        {
            // GetState: ask the app for its current state position. Only called when
            // we don't have a saved position (first subscribe or after a position reset
            // due to unrecoverable position error 112). On normal reconnects with a
            // valid saved position we skip GetState and let the server try recovery —
            // GetState is only called again if recovery fails.
            bool needGetState;
            lock (_stateChangeLock)
            {
                if (!IsCurrentAttempt(epoch, connectionGeneration)) return null;
                needGetState = _options.GetState != null && _streamPosition == null;
            }
            if (needGetState)
            {
                CentrifugeStreamPosition position;
                try
                {
                    position = await _options.GetState!(Channel).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    throw new CentrifugeGetStateException(ex);
                }
                lock (_stateChangeLock)
                {
                    if (!IsCurrentAttempt(epoch, connectionGeneration)) return null;
                    _streamPosition = position;
                    _positionBase++;
                }
            }

            string? token;
            bool needsRefresh;
            lock (_stateChangeLock)
            {
                if (!IsCurrentAttempt(epoch, connectionGeneration)) return null;
                token = _token;
                needsRefresh = _refreshRequired;
            }

            // If refresh is required or token is empty, try to get a new token
            if ((string.IsNullOrEmpty(token) || needsRefresh) && _options.GetToken != null)
            {
                try
                {
                    token = await _options.GetToken(Channel).ConfigureAwait(false);
                    if (string.IsNullOrEmpty(token)) throw new CentrifugeUnauthorizedException();
                    lock (_stateChangeLock)
                    {
                        if (!IsCurrentAttempt(epoch, connectionGeneration)) return null;
                        _token = token;
                    }
                }
                catch (CentrifugeUnauthorizedException)
                {
                    await SetUnsubscribedAsync(CentrifugeUnsubscribedCodes.Unauthorized, "unauthorized", sendUnsubscribe: false, epoch, connectionGeneration).ConfigureAwait(false);
                    return null;
                }
                catch (Exception ex)
                {
                    throw new CentrifugeException(CentrifugeErrorCodes.SubscriptionSubscribeToken, ex.Message, true, ex);
                }
            }

            ReadOnlyMemory<byte> data;
            CentrifugeFilterNode? tagsFilter;
            lock (_stateChangeLock)
            {
                data = _data;
                tagsFilter = _tagsFilter;
            }

            var request = new SubscribeRequest
            {
                Channel = Channel,
                Token = token ?? string.Empty,
                Positioned = _options.Positioned,
                Recoverable = _options.Recoverable,
                JoinLeave = _options.JoinLeave
            };

            CentrifugeStreamPosition? streamPos;
            lock (_stateChangeLock) { streamPos = RecoveryPositionLocked(); }
            if (streamPos != null)
            {
                request.Recover = true;
                request.Epoch = streamPos.Value.Epoch;
                request.Offset = streamPos.Value.Offset;
            }

            if (!data.IsEmpty)
            {
                request.Data = ByteString.CopyFrom(data.Span);
            }

            if (tagsFilter != null)
            {
                request.Tf = tagsFilter.InternalNode;
            }

            if (!string.IsNullOrEmpty(_options.Delta))
            {
                request.Delta = _options.Delta;
            }

            // Always offer channel compaction: when the server supports and allows it,
            // the subscribe result carries a numeric channel ID and subsequent pushes
            // use that ID instead of the string channel name.
            long flag = CentrifugeSubscriptionFlags.ChannelCompaction;
            if (_options.GetState != null)
            {
                // Ask the server to reject the subscribe with error 112 when recovery
                // from the provided position is impossible, instead of returning
                // recovered=false — so we can call GetState again to reload state.
                flag |= CentrifugeSubscriptionFlags.RejectUnrecovered;
            }
            request.Flag = flag;

            return new Command
            {
                Id = _client.NextCommandId(),
                Subscribe = request
            };
        }

        /// <summary>
        /// Resets the cached state on "state invalidated" (unsubscribe code 2502 or disconnect code
        /// 3014) so the resubscribe re-syncs: the token when GetToken can replace it (a static token
        /// stays, as centrifuge-js), the fossil delta base (a stale one would corrupt decoding of the
        /// first publication) and the channel compaction ID. A recovery position is reset to the
        /// sentinel epoch "_" the server can never match: the reply reports WasRecovering=true,
        /// Recovered=false, and the app reloads through its recovery-failure path.
        /// </summary>
        internal void InvalidateState()
        {
            lock (_stateChangeLock)
            {
                if (_options.GetToken != null)
                {
                    _token = string.Empty;
                    _refreshRequired = true;
                }
                if (_streamPosition != null)
                {
                    _streamPosition = new CentrifugeStreamPosition(0, "_");
                    _positionBase++;
                }
                lock (_deltaLock) { _prevValue = null; }
                SetPushChannelId(0);
            }
        }

        /// <summary>
        /// Update the channel compaction ID registration in the client's push
        /// routing registry. Pass 0 to clear (no compaction / sub gone). The field and the
        /// registry change together under _stateChangeLock, so a concurrent clear (unsubscribe)
        /// and register (subscribe reply) can't leave an entry for an unsubscribed subscription;
        /// the client's lock is taken inside it (see CentrifugeClient.UpdateSubscriptionPushId).
        ///
        /// Always re-registers even when the ID is unchanged: the client drops the
        /// whole registry on transport teardown, and on reconnect the server commonly
        /// assigns the same ID again — the registration must be restored. An ID of
        /// Connected session <paramref name="connectionGeneration"/> that has ended isn't kept.
        /// </summary>
        private void SetPushChannelId(long id, long connectionGeneration = 0)
        {
            lock (_stateChangeLock)
            {
                var oldId = _pushChannelId;
                if (id == 0 && oldId == 0) return;
                _pushChannelId = _client.UpdateSubscriptionPushId(this, oldId, id, connectionGeneration) ? id : 0;
            }
        }

        /// <summary>
        /// Applies a successful subscribe reply of the attempt identified by
        /// <paramref name="epoch"/>. Runs on the transport receive loop (see
        /// SendSubscribeIfNeededAsync), so Subscribed and the recovered publications are
        /// raised before any publication that follows the reply. The events stop once the
        /// subscription moved on (a handler unsubscribed), or at a recovered publication that fails
        /// to decode (see TryDecode). The stream position records what the app received:
        /// with recovered publications it stays where recovery started and advances with each
        /// delivered one, so a delivery cut short resumes there; otherwise it moves to the top right
        /// before Subscribed is raised (a handler may reconnect at once), so a first subscribe or a
        /// failed recovery whose Subscribed never reached the app reports its base again. Only a
        /// recoverable reply keeps a position to recover from — otherwise recovery stops — and
        /// Subscribed reports the server's was_recovering and the reply's position of a positioned or
        /// recoverable channel (as centrifuge-js). Returns
        /// false when the reply was discarded — the attempt was superseded
        /// (its unsubscribe removed the server-side subscription, so a newer attempt must not
        /// adopt the reply) or its connection has been torn down — and the caller should
        /// retry, because a resubscribe sweep may have already run and skipped this
        /// subscription while the reply was inflight. Internal for tests.
        /// <para>
        /// One critical section spans the attempt and session checks, the transition, the
        /// channel compaction ID and the resolve of the ready waiters: a ReadyAsync caller can't miss it,
        /// and a concurrent Unsubscribe (a new epoch) either discards the reply or clears the ID
        /// after it. The connection generation is bumped under the client's lock before
        /// subscriptions move to Subscribing, so a matching one means the teardown hasn't touched
        /// the subscription yet; the teardown clears the ID registry before that, so the ID is
        /// registered only after a recheck under the client's lock. The delta chain restarts (the
        /// server's first publication is full).
        /// A reply after Dispose is dropped: nothing to retry.
        /// </para>
        /// </summary>
        internal bool HandleSubscribeReply(SubscribeResult result, int epoch, long connectionGeneration, long attempt)
        {
            if (System.Threading.Interlocked.CompareExchange(ref _disposed, 0, 0) != 0) return true;

            bool recovered = result.Recovered;
            CentrifugeStreamPosition? streamPositionSnapshot;
            CentrifugeStreamPosition? topPosition = null;
            long positionBase;
            bool continuesStream;
            CentrifugeSubscriptionState prevState;
            int subscribedEpoch;
            lock (_stateChangeLock)
            {
                ReleaseInflightLocked(attempt);

                if (_epoch != epoch)
                {
                    return false;
                }

                if (connectionGeneration != _client.ConnectionGeneration)
                {
                    return false;
                }

                if (result.Recoverable)
                    topPosition = new CentrifugeStreamPosition(result.Offset, result.Epoch);
                streamPositionSnapshot = result.Positioned || result.Recoverable
                    ? new CentrifugeStreamPosition(result.Offset, result.Epoch)
                    : null;
                continuesStream = _streamPosition != null && recovered && result.Publications.Count > 0
                    && _streamPosition?.Epoch == result.Epoch;
                if (!result.Recoverable && _streamPosition != null)
                {
                    _streamPosition = null;
                    _positionBase++;
                }
                positionBase = _positionBase;

                lock (_deltaLock)
                {
                    _deltaNegotiated = result.Delta;
                    _prevValue = null;
                }

                ClearRefreshTimer();
                _refreshAttempts = 0;
                _refreshRequired = false;

                prevState = SetState(CentrifugeSubscriptionState.Subscribed);
                subscribedEpoch = _epoch;
                _subscribedGeneration = connectionGeneration;
                if (result.Expires)
                {
                    ScheduleTokenRefresh(result.Ttl, subscribedEpoch, connectionGeneration);
                }
                _resubscribeAttempts = 0;
                SetPushChannelId(result.Id, connectionGeneration);

                _readyPromises.ResolveAll();
            }

            Raise(StateChanged, new CentrifugeSubscriptionStateEventArgs(prevState, CentrifugeSubscriptionState.Subscribed), "stateChanged");

            if (!IsEpoch(subscribedEpoch)) return true;
            if (topPosition != null && !continuesStream)
                AdoptReportedBase(ref positionBase, topPosition.Value);
            if (Subscribed != null)
            {
                Raise(Subscribed, new CentrifugeSubscribedEventArgs(
                    result.WasRecovering,
                    recovered,
                    result.Recoverable,
                    result.Positioned,
                    streamPositionSnapshot,
                    result.Data.ToByteArray()
                ), "subscribed");
            }

            foreach (var pub in result.Publications)
            {
                if (!IsEpoch(subscribedEpoch)) return true;

                if (!TryDecode(pub, out var args)) return true;
                Deliver(args, positionBase, pub.Offset, pub.Epoch);
            }

            if (topPosition is { } top) AdvanceDeliveredPosition(positionBase, top.Offset, top.Epoch);
            return true;
        }

        /// <summary>
        /// Makes <paramref name="top"/>, the base Subscribed reports, the position as it is raised, unless a
        /// newer base replaced <paramref name="positionBase"/>. Until then a subscribe whose Subscribed
        /// was never raised reports the base again.
        /// </summary>
        private void AdoptReportedBase(ref long positionBase, CentrifugeStreamPosition top)
        {
            lock (_stateChangeLock)
            {
                if (_positionBase != positionBase) return;
                _streamPosition = top;
                positionBase = ++_positionBase;
            }
        }

        /// <summary>Advances the position to <paramref name="offset"/> within <paramref name="positionBase"/>,
        /// unless a newer base replaced it.</summary>
        private void AdvanceDeliveredPosition(long positionBase, ulong offset, string epoch)
        {
            lock (_stateChangeLock) AdvancePositionLocked(positionBase, offset, epoch);
        }

        private void AdvancePositionLocked(long positionBase, ulong offset, string epoch)
        {
            if (_positionBase == positionBase && _streamPosition?.Past(offset, epoch) is { } next)
                _streamPosition = next;
        }

        /// <summary>Raises a publication and advances the position past it within <paramref name="positionBase"/>
        /// (null — the current); a subscribe its handler starts recovers past it (RecoveryPositionLocked, as
        /// centrifuge-js _pendingOffset).</summary>
        private void Deliver(CentrifugePublicationEventArgs args, long? positionBase, ulong offset, string epoch)
        {
            if (offset == 0)
            {
                Raise(Publication, args, "publication");
                return;
            }

            long seq;
            long deliveringBase;
            lock (_stateChangeLock)
            {
                seq = ++_deliverySeq;
                deliveringBase = positionBase ?? _positionBase;
                _delivering = (seq, deliveringBase, offset, epoch);
            }
            Raise(Publication, args, "publication");
            lock (_stateChangeLock)
            {
                if (_delivering?.Seq == seq) _delivering = null;
                AdvancePositionLocked(deliveringBase, offset, epoch);
            }
        }

        /// <summary>The position to recover from: the stored one, past a publication being delivered. Call
        /// under _stateChangeLock.</summary>
        private CentrifugeStreamPosition? RecoveryPositionLocked() =>
            _delivering is { } delivering && delivering.Base == _positionBase && _streamPosition is { } current
                ? current.Past(delivering.Offset, delivering.Epoch) ?? current
                : _streamPosition;

        /// <summary>
        /// A live publication. Pushes reach a subscription only while it is Subscribed: one routed by
        /// channel name to a subscription that moved on (a handler unsubscribed mid-frame) would
        /// advance the position past recovered publications it never delivered. The position advances
        /// within the base it had when the delivery started (Deliver). Returns false when not Subscribed:
        /// the push isn't this subscription's.
        /// </summary>
        internal bool HandlePublication(Publication pub)
        {
            if (_state != CentrifugeSubscriptionState.Subscribed) return false;
            if (!TryDecode(pub, out var args)) return true;
            Deliver(args, null, pub.Offset, pub.Epoch);
            return true;
        }

        /// <summary>
        /// Decodes a publication dispatched from the receive loop. One that fails to decode means a
        /// corrupt stream: it raises Error("publicationDecode" — "publication" is a handler's exception)
        /// and the client disconnects the session of the dispatching
        /// transport with BadProtocol, without reconnecting (as current centrifuge-js), so nothing is
        /// delivered past it.
        /// </summary>
        private bool TryDecode(Publication pub,
            [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out CentrifugePublicationEventArgs? args)
        {
            try
            {
                args = ApplyDeltaIfNeeded(pub);
                return true;
            }
            catch (Exception ex)
            {
                args = null;
                OnError("publicationDecode", ex);
                _ = _client.HandleUndecodablePublicationAsync();
                return false;
            }
        }

        private CentrifugePublicationEventArgs ApplyDeltaIfNeeded(Publication pub)
        {
            var data = pub.Data.ToByteArray();

            // Hold _deltaLock (not _stateChangeLock) across the full read-apply-write sequence so concurrent callers
            // (HandleSubscribeReply recovery loop + HandlePublication live stream) can't
            // both read the same _prevValue and then corrupt the delta chain with two
            // independent writes. A dedicated lock keeps O(payload-size) Fossil decode
            // off the hot state-change path.
            if (!string.IsNullOrEmpty(_options.Delta))
            {
                lock (_deltaLock)
                {
                    if (_deltaNegotiated)
                    {
                        if (pub.Delta)
                            data = Fossil.ApplyDelta(_prevValue ?? throw new System.IO.InvalidDataException("delta publication without a base"), data);
                        _prevValue = data;
                    }
                }
            }

            return CentrifugeClient.CreatePublicationArgs(Channel, pub, data);
        }

        /// <summary>A join push; taken only while Subscribed, as publications.</summary>
        internal bool HandleJoin(Join join)
        {
            if (_state != CentrifugeSubscriptionState.Subscribed) return false;

            if (join.Info != null && Join != null)
                Raise(Join, new CentrifugeJoinEventArgs(Channel, CentrifugeClientInfo.FromProtocol(join.Info)), "join");
            return true;
        }

        /// <summary>A leave push; taken only while Subscribed, as publications.</summary>
        internal bool HandleLeave(Leave leave)
        {
            if (_state != CentrifugeSubscriptionState.Subscribed) return false;

            if (leave.Info != null && Leave != null)
                Raise(Leave, new CentrifugeLeaveEventArgs(Channel, CentrifugeClientInfo.FromProtocol(leave.Info)), "leave");
            return true;
        }

        /// <summary>
        /// Waits the resubscribe backoff after the attempt identified by <paramref name="epoch"/> and
        /// <paramref name="connectionGeneration"/>; a superseded one hands over at once. The check and
        /// the arming are one critical section: a teardown after them cancels the wait (MoveToSubscribing),
        /// so a stale backoff never holds off the next session. Returns false when the wait was cancelled
        /// (a newer one replaced it, or the subscription moved on) or the subscription left Subscribing.
        /// A wait that ran out is released, so the next attempt can run.
        /// </summary>
        private async Task<bool> DelayResubscribeAsync(int epoch, long connectionGeneration)
        {
            CancellationToken delayToken;
            int currentAttempts;
            lock (_stateChangeLock)
            {
                if (_state != CentrifugeSubscriptionState.Subscribing) return false;
                if (!IsCurrentAttempt(epoch, connectionGeneration)) return true;
                // Don't recreate _resubscribeCts after Dispose has nulled it — that would leak the CTS.
                if (System.Threading.Interlocked.CompareExchange(ref _disposed, 0, 0) != 0) return false;

                var oldCts = _resubscribeCts;
                _resubscribeCts = new CancellationTokenSource();
                _resubscribeGeneration = connectionGeneration;
                oldCts?.Cancel();
                oldCts?.Dispose();
                delayToken = _resubscribeCts.Token;
                currentAttempts = _resubscribeAttempts++;
            }

            int delay = Utilities.CalculateBackoff(
                currentAttempts,
                _options.MinResubscribeDelay,
                _options.MaxResubscribeDelay
            );

            try
            {
                await Task.Delay(delay, delayToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return false;
            }

            lock (_stateChangeLock)
            {
                if (_resubscribeCts is { } cts && cts.Token == delayToken)
                {
                    _resubscribeCts = null;
                    cts.Dispose();
                }
                return _state == CentrifugeSubscriptionState.Subscribing;
            }
        }

        /// <summary>
        /// Whether the attempt identified by <paramref name="epoch"/>, sent on the
        /// connection session <paramref name="connectionGeneration"/>, is still current.
        /// Call under _stateChangeLock.
        /// </summary>
        private bool IsCurrentAttempt(int epoch, long connectionGeneration) =>
            _epoch == epoch && _client.ConnectionGeneration == connectionGeneration;

        /// <summary>Whether the subscription is still in the state epoch <paramref name="epoch"/>.</summary>
        private bool IsEpoch(int epoch)
        {
            lock (_stateChangeLock)
            {
                return _epoch == epoch;
            }
        }

        /// <summary>
        /// Moves the subscription to Unsubscribed. The unsubscribe command is queued in the
        /// critical section of the transition, before any handler runs and bound to the
        /// Connected session, so it reaches the server ahead of any later subscribe (see
        /// SendSubscribeIfNeededAsync). Only a Subscribed subscription or one with a subscribe
        /// inflight, and only in a Connected session, may have a server-side counterpart; otherwise
        /// nothing is sent (as centrifuge-js): a needless command could outlast its timeout and force
        /// a reconnect. An unsubscribe with an error reply or a timeout reconnects its session; one whose
        /// session is gone or ending (a failed write ends it through the transport, with the server's
        /// close code when one arrived) is left to that teardown.
        /// Unsubscribed is raised only while the StateChanged handler didn't move the subscription on.
        /// </summary>
        /// <param name="code">Unsubscribe code.</param>
        /// <param name="reason">Unsubscribe reason.</param>
        /// <param name="sendUnsubscribe">The server may still hold the subscription (a local
        /// unsubscribe, a failed refresh); false when the server ended or refused it or the
        /// client closed (as centrifuge-js).</param>
        /// <param name="epoch">When set, unsubscribe only if the subscription is still in
        /// that epoch — the outcome of an attempt or session that is not superseded.</param>
        /// <param name="connectionGeneration">When set, unsubscribe only if the client is
        /// still in that connection session.</param>
        internal async Task SetUnsubscribedAsync(int code, string reason, bool sendUnsubscribe, int? epoch = null, long? connectionGeneration = null)
        {
            CentrifugeSubscriptionState prevState;
            int unsubscribedEpoch;
            Task<Reply>? unsubscribe = null;
            long unsubscribeGeneration = 0;
            lock (_stateChangeLock)
            {
                if (_state == CentrifugeSubscriptionState.Unsubscribed ||
                    (epoch.HasValue && _epoch != epoch.Value) ||
                    (connectionGeneration.HasValue && _client.ConnectionGeneration != connectionGeneration.Value))
                {
                    return;
                }

                bool serverSide = _state == CentrifugeSubscriptionState.Subscribed ||
                    (_state == CentrifugeSubscriptionState.Subscribing && _inflightAttempt != 0);
                prevState = SetState(CentrifugeSubscriptionState.Unsubscribed);
                unsubscribedEpoch = _epoch;

                // Reject ready promises
                _readyPromises.RejectAll(new CentrifugeException(CentrifugeErrorCodes.SubscriptionUnsubscribed, "subscription unsubscribed"));

                // Channel compaction ID is no longer valid once unsubscribed.
                SetPushChannelId(0);
                _resubscribeCts?.Cancel();

                if (sendUnsubscribe && serverSide && _client.ConnectedSession is { } session)
                {
                    unsubscribeGeneration = session.Generation;
                    unsubscribe = _client.SendCommandAsync(new Command
                    {
                        Id = _client.NextCommandId(),
                        Unsubscribe = new UnsubscribeRequest
                        {
                            Channel = Channel
                        }
                    }, session.Transport, null, CancellationToken.None);
                }
            }

            Raise(StateChanged, new CentrifugeSubscriptionStateEventArgs(prevState, CentrifugeSubscriptionState.Unsubscribed), "stateChanged");
            if (IsEpoch(unsubscribedEpoch))
                Raise(Unsubscribed, new CentrifugeUnsubscribedEventArgs(code, reason), "unsubscribed");

            if (unsubscribe == null) return;
            bool failed;
            try
            {
                failed = (await unsubscribe.ConfigureAwait(false)).Error != null;
            }
            catch (CentrifugeException ex) when (
                ex.Code == CentrifugeErrorCodes.ClientDisconnected ||
                ex.Code == CentrifugeErrorCodes.ConnectionClosed ||
                ex.Code == CentrifugeErrorCodes.TransportWriteError)
            {
                return;
            }
            catch
            {
                failed = true;
            }
            if (failed) _client.HandleUnsubscribeError(unsubscribeGeneration);
        }

        /// <summary>Changes the state under _stateChangeLock; leaving Subscribed drops the token refresh
        /// armed for it (as centrifuge-js _clearSubscribedState).</summary>
        private CentrifugeSubscriptionState SetState(CentrifugeSubscriptionState newState)
        {
            var oldState = _state;
            _state = newState;
            if (oldState != newState) _epoch++;
            if (oldState == CentrifugeSubscriptionState.Subscribed && newState != oldState) ClearRefreshTimer();
            return oldState;
        }

        private void OnError(string type, Exception exception) =>
            EventDispatch.RaiseError(this, Error, type, exception, _client.Logger);

        /// <summary>See <see cref="EventDispatch.Raise{TArgs}"/>.</summary>
        private void Raise<TArgs>(EventHandler<TArgs>? handler, TArgs args, string type) =>
            EventDispatch.Raise(this, handler, args, type, Error, _client.Logger);

        private void ScheduleTokenRefresh(uint ttl, int epoch, long generation)
        {
            ClearRefreshTimer();
            // ttl=0 means "no expiry given" — skip scheduling rather than busy-loop.
            if (ttl == 0) return;
            ArmRefreshTimer(Utilities.TtlToMilliseconds(ttl), epoch, generation);
        }

        /// <summary>Retries the refresh of the Subscribed state <paramref name="epoch"/> and connection session <paramref name="generation"/> with backoff.</summary>
        private void ScheduleRefreshRetry(int epoch, long generation)
        {
            lock (_stateChangeLock)
            {
                if (!IsCurrentRefresh(epoch, generation)) return;
                ArmRefreshTimer(Utilities.CalculateBackoff(_refreshAttempts++, _options.MinResubscribeDelay, _options.MaxResubscribeDelay), epoch, generation);
            }
        }

        /// <summary>
        /// Replaces the refresh timer of the Subscribed state <paramref name="epoch"/> and connection
        /// session <paramref name="generation"/>: a callback of a state or session that ended does
        /// nothing. Call under _stateChangeLock: Dispose takes the timer under it after marking the
        /// subscription disposed, so a disposed subscription arms none.
        /// </summary>
        private void ArmRefreshTimer(int delay, int epoch, long generation)
        {
            ClearRefreshTimer();
            if (Interlocked.CompareExchange(ref _disposed, 0, 0) != 0) return;
            _refreshTimer = new Timer(_ => _ = RefreshTokenAsync(epoch, generation), null, delay, Timeout.Infinite);
        }

        private void ClearRefreshTimer()
        {
            _refreshTimer?.Dispose();
            _refreshTimer = null;
        }

        /// <summary>
        /// Refreshes the subscription token of the Subscribed state <paramref name="epochSnapshot"/> and
        /// connection session <paramref name="generation"/> its timer was armed in (IsCurrentRefresh): a
        /// refresh of a subscription or session that moved on does nothing, and its outcome changes
        /// nothing, not even an Error event. Without GetToken the expiring token can't be replaced: a
        /// configuration error, then Unauthorized (as centrifuge-js); a null token is Unauthorized.
        /// </summary>
        internal async Task RefreshTokenAsync(int epochSnapshot, long generation)
        {
            lock (_stateChangeLock)
            {
                if (!IsCurrentRefresh(epochSnapshot, generation)) return;
            }

            if (_options.GetToken == null)
            {
                OnError("configuration", new CentrifugeConfigurationException("subscription token expired but no GetToken is set"));
                await SetUnsubscribedAsync(CentrifugeUnsubscribedCodes.Unauthorized, "unauthorized", sendUnsubscribe: true, epochSnapshot, generation).ConfigureAwait(false);
                return;
            }

            string token;
            try
            {
                token = await _options.GetToken(Channel).ConfigureAwait(false);
            }
            catch (CentrifugeUnauthorizedException)
            {
                await SetUnsubscribedAsync(CentrifugeUnsubscribedCodes.Unauthorized, "unauthorized", sendUnsubscribe: true,
                    epochSnapshot, generation).ConfigureAwait(false);
                return;
            }
            catch (Exception ex)
            {
                FailRefresh("refreshToken", new CentrifugeException(CentrifugeErrorCodes.SubscriptionRefreshToken, ex.Message, true, ex),
                    epochSnapshot, generation);
                return;
            }
            if (string.IsNullOrEmpty(token))
            {
                await SetUnsubscribedAsync(CentrifugeUnsubscribedCodes.Unauthorized, "unauthorized", sendUnsubscribe: true, epochSnapshot, generation).ConfigureAwait(false);
                return;
            }

            Transports.ITransport transport;
            lock (_stateChangeLock)
            {
                if (!IsCurrentRefresh(epochSnapshot, generation)) return;
                if (_client.ConnectedSession is not { } connected || connected.Generation != generation) return;
                _token = token;
                transport = connected.Transport;
            }

            try
            {
                var reply = await _client.SendCommandAsync(new Command
                {
                    Id = _client.NextCommandId(),
                    SubRefresh = new SubRefreshRequest { Channel = Channel, Token = token }
                }, transport, null, CancellationToken.None).ConfigureAwait(false);

                if (reply.Error != null)
                {
                    HandleRefreshError(CentrifugeException.FromReply(reply.Error), epochSnapshot, generation);
                }
                else
                {
                    HandleRefreshReply(reply.SubRefresh, epochSnapshot, generation);
                }
            }
            catch (Exception ex)
            {
                FailRefresh("refresh", ex, epochSnapshot, generation);
            }
        }

        /// <summary>A failed refresh of a current Subscribed state and session: reported, then retried with backoff.</summary>
        private void FailRefresh(string type, Exception error, int epoch, long generation)
        {
            lock (_stateChangeLock)
            {
                if (!IsCurrentRefresh(epoch, generation)) return;
            }
            OnError(type, error);
            ScheduleRefreshRetry(epoch, generation);
        }

        /// <summary>Whether a refresh started in the Subscribed state <paramref name="epoch"/> and connection
        /// session <paramref name="generation"/> is still current. Call under _stateChangeLock.</summary>
        private bool IsCurrentRefresh(int epoch, long generation) =>
            _state == CentrifugeSubscriptionState.Subscribed && IsCurrentAttempt(epoch, generation);

        private void HandleRefreshReply(SubRefreshResult result, int epoch, long generation)
        {
            lock (_stateChangeLock)
            {
                if (!IsCurrentRefresh(epoch, generation)) return;
                _refreshAttempts = 0;
                if (result.Expires) ScheduleTokenRefresh(result.Ttl, epoch, generation);
                else ClearRefreshTimer();
            }
        }

        private void HandleRefreshError(CentrifugeException error, int epoch, long generation)
        {
            lock (_stateChangeLock)
            {
                if (!IsCurrentRefresh(epoch, generation)) return;
            }
            OnError("refresh", error);

            if (error.Temporary)
            {
                ScheduleRefreshRetry(epoch, generation);
            }
            else
            {
                _ = SetUnsubscribedAsync(error.Code, error.Message, sendUnsubscribe: true, epoch, generation);
            }
        }


        /// <summary>
        /// Unsubscribes (as Unsubscribe()), removes the subscription from its client and releases its
        /// timers; a disposed subscription can't subscribe again.
        /// </summary>
        public void Dispose()
        {
            if (System.Threading.Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;

            _ = SetUnsubscribedAsync(CentrifugeUnsubscribedCodes.UnsubscribeCalled, "unsubscribe called", sendUnsubscribe: true);
            _client.Unregister(this);

            CancellationTokenSource? cts;
            Timer? timer;
            lock (_stateChangeLock)
            {
                cts = _resubscribeCts;
                _resubscribeCts = null;
                timer = _refreshTimer;
                _refreshTimer = null;
            }

            try { cts?.Cancel(); } catch (ObjectDisposedException) { }
            cts?.Dispose();
            timer?.Dispose();
        }
    }
}
