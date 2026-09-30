using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Centrifugal.Centrifuge;
using Centrifugal.Centrifuge.Protocol;
using Centrifugal.Centrifuge.Transports;
using Google.Protobuf;
using Xunit;

namespace Centrifugal.Centrifuge.Tests
{
    /// <summary>
    /// Ordering of connect/subscribe replies relative to the frames that follow them
    /// on the same transport, and binding of asynchronous outcomes (replies, errors,
    /// GetState/GetToken results, refreshes, connect failures) to the subscribe or
    /// connect attempt that produced them.
    /// <para>
    /// An outcome that must not happen is checked once the stage that would produce it
    /// is complete, not after a delay: a later command reached the server (a session's
    /// commands arrive in queue order), a later frame was dispatched, the transport's
    /// own handler saw the frame (<see cref="FrameProcessed"/>), a test-driven SDK call
    /// returned, or a test-owned source was released inline (<see cref="CompleteInline"/>).
    /// A command timer, which has no other observable end, is held past by the clock
    /// (<see cref="TimeoutHold"/>). <see cref="Wait"/> only bounds the waits.
    /// </para>
    /// <para>
    /// Frame gate (<see cref="FrameGate"/>): a handler raised while a reply is applied
    /// runs either on the receive loop dispatching the frame, which it then holds, so
    /// the rest of the frame follows in protocol order; or elsewhere, and then it waits
    /// for the competing event of the frame, which overtakes it and breaks the recorded
    /// order.
    /// </para>
    /// </summary>
    [Collection("Integration")]
    public class OrderedReplyTests : IAsyncLifetime
    {
        private static readonly TimeSpan Wait = TimeSpan.FromSeconds(5);
        /// <summary>Resubscribe backoff far above Wait: a test passes only if the SDK retries without it.</summary>
        private static readonly TimeSpan NoBackoff = TimeSpan.FromSeconds(30);
        private const BindingFlags Private = BindingFlags.NonPublic | BindingFlags.Instance;

        private readonly FakeCentrifugoServer _server = new();
        private CentrifugeClient? _client;
        private readonly List<string> _order = new();
        private int _frameThread;

        public Task InitializeAsync() => _server.StartAsync();

        public async Task DisposeAsync()
        {
            if (_client != null) await _client.DisposeAsync();
            await _server.DisposeAsync();
        }

        private CentrifugeClient NewClient(CentrifugeClientOptions? options = null)
        {
            options ??= new CentrifugeClientOptions();
            options.MinReconnectDelay = TimeSpan.FromMilliseconds(1);
            options.MaxReconnectDelay = TimeSpan.FromMilliseconds(50);
            return _client = new CentrifugeClient(_server.Url, options);
        }

        private static CentrifugeSubscriptionOptions WithoutBackoff() => new()
        {
            MinResubscribeDelay = NoBackoff,
            MaxResubscribeDelay = NoBackoff,
        };

        private void Record(string entry)
        {
            lock (_order) _order.Add(entry);
        }

        private string[] Recorded()
        {
            lock (_order) return _order.ToArray();
        }

        private static Publication Pub(ulong offset, string data) =>
            new() { Offset = offset, Data = ByteString.CopyFromUtf8(data) };

        private static Publication DeltaPub(ulong offset, string delta) =>
            new() { Offset = offset, Data = ByteString.CopyFromUtf8(delta), Delta = true };

        /// <summary>Holds the first subscribe command unanswered and hands it to the test.</summary>
        private TaskCompletionSource<Command> HoldFirstSubscribe()
        {
            var held = new TaskCompletionSource<Command>(TaskCreationOptions.RunContinuationsAsynchronously);
            _server.OnCommand = cmd =>
                cmd.Subscribe != null && held.TrySetResult(cmd) ? FakeCentrifugoServer.NoReply : null;
            return held;
        }

        private static async Task Until(Func<bool> condition)
        {
            await Task.Run(async () =>
            {
                while (!condition()) await Task.Delay(10);
            }).WaitAsync(Wait);
        }

        private static object? Field(object owner, string name) => owner.GetType().GetField(name, Private)!.GetValue(owner);

        private static ITransport CurrentTransport(CentrifugeClient client) => (ITransport)Field(client, "_transport")!;

        /// <summary>
        /// Completes once the client's current transport received a frame and the client's own
        /// handler, subscribed before, processed it — dispatched or dropped it.
        /// </summary>
        private static Task FrameProcessed(CentrifugeClient client)
        {
            var processed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            CurrentTransport(client).MessageReceived += (_, _) => processed.TrySetResult();
            return processed.Task;
        }

        /// <summary>
        /// Completes a test-owned source created without RunContinuationsAsynchronously off the
        /// test's synchronization context, where the SDK's awaits on it (ConfigureAwait(false))
        /// resume inline: when this returns, the SDK has run up to its next pending await. The SDK
        /// must already await the source (see UntilAwaited).
        /// </summary>
        private static Task CompleteInline(Action complete) => Task.Run(complete);

        /// <summary>Waits until <paramref name="task"/> has a continuation: a source handed to the SDK is
        /// awaited only after the call that returned it.</summary>
        private static Task UntilAwaited(Task task) => Until(() => TaskContinuation.GetValue(task) != null);

        private static readonly FieldInfo TaskContinuation = typeof(Task).GetField("m_continuationObject", Private)!;

        /// <summary>Opens a frame: its Message event records the thread the receive loop dispatches the frame on.</summary>
        private static Reply FrameMarker() =>
            new() { Push = new Push { Message = new Message { Data = ByteString.CopyFromUtf8("frame") } } };

        private void TrackFrameThread(CentrifugeClient client) =>
            client.Message += (_, _) => Volatile.Write(ref _frameThread, Environment.CurrentManagedThreadId);

        /// <summary>
        /// Waits for <paramref name="competing"/> unless the caller runs on the receive loop dispatching
        /// the frame opened by <see cref="FrameMarker"/> (a thread that finished that dispatch has
        /// already dispatched the competing event, the recorded order shows it).
        /// </summary>
        private void FrameGate(ManualResetEventSlim competing)
        {
            if (Environment.CurrentManagedThreadId != Volatile.Read(ref _frameThread)) competing.Wait(Wait);
        }

        /// <summary>Command timeout of the tests holding a claimed reply past it: ample for a loopback frame to be claimed first.</summary>
        private static readonly TimeSpan CommandTimeout = TimeSpan.FromSeconds(2);

        /// <summary>
        /// Holds the receive loop in a handler raised after the claim of a frame's replies until a command's
        /// timer has run out. Created before the command is issued, so on its clock the timer can't fire before
        /// <see cref="CommandTimeout"/> less <see cref="TimerSlack"/> (timer tick granularity): a handler entered
        /// earlier saw the reply claimed before the timer fired (asserted). The hold lasts until the timer's due
        /// time, counted from when the server got the command (<see cref="Armed"/>), plus a margin for the SDK's
        /// continuations: the one arming the timer once the command is written, and the one on the fired timer.
        /// </summary>
        private sealed class TimeoutHold
        {
            private static readonly TimeSpan TimerSlack = TimeSpan.FromMilliseconds(50);
            private static readonly TimeSpan TimerReaction = TimeSpan.FromMilliseconds(500);
            private readonly Stopwatch _clock = Stopwatch.StartNew();
            private readonly TaskCompletionSource<TimeSpan> _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly ManualResetEventSlim _released = new();
            private TimeSpan? _armed;

            /// <summary>Called once the server got the command.</summary>
            public void Armed() => _armed = _clock.Elapsed;

            /// <summary>Called by the handler on the receive loop: holds it until <see cref="Release"/>.</summary>
            public void Hold()
            {
                _entered.TrySetResult(_clock.Elapsed);
                _released.Wait(Wait);
            }

            /// <summary>Completes once the handler, entered before the timer fired, has held the receive loop past it.</summary>
            public async Task PastTimeoutAsync()
            {
                var entered = await _entered.Task.WaitAsync(Wait);
                Assert.True(entered < CommandTimeout - TimerSlack,
                    $"Reply claimed {entered.TotalMilliseconds:F0} ms after the command was issued, not before its timeout");
                TimeSpan left;
                while ((left = _armed!.Value + CommandTimeout + TimerReaction - _clock.Elapsed) > TimeSpan.Zero)
                    await Task.Delay(left);
            }

            public void Release() => _released.Set();
        }

        /// <summary>Frame gate on the transition to Subscribed; the live publication competes.</summary>
        [Fact]
        public async Task LivePublicationDoesNotOvertakeSubscribedAndRecovered()
        {
            var held = HoldFirstSubscribe();
            var client = NewClient();
            TrackFrameThread(client);
            client.Connect();
            await client.ReadyAsync(Wait);

            var liveSeen = new ManualResetEventSlim();
            var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var sub = client.NewSubscription("ordered");
            sub.StateChanged += (_, e) =>
            {
                if (e.NewState != CentrifugeSubscriptionState.Subscribed) return;
                Record("state");
                FrameGate(liveSeen);
            };
            sub.Subscribed += (_, _) => Record("subscribed");
            sub.Publication += (_, e) =>
            {
                Record("pub" + e.Offset);
                if (e.Offset != 3) return;
                liveSeen.Set();
                done.TrySetResult();
            };
            sub.Subscribe();

            var cmd = await held.Task.WaitAsync(Wait);
            await _server.SendRepliesAsync(
                FrameMarker(),
                new Reply
                {
                    Id = cmd.Id,
                    Subscribe = new SubscribeResult
                    {
                        Recoverable = true, Positioned = true, Epoch = "e", Offset = 2, Recovered = true,
                        Publications = { Pub(1, "r1"), Pub(2, "r2") },
                    },
                },
                new Reply { Push = new Push { Channel = "ordered", Pub = Pub(3, "live") } });

            await done.Task.WaitAsync(Wait);
            Assert.Equal(new[] { "state", "subscribed", "pub1", "pub2", "pub3" }, Recorded());
        }

        /// <summary>Frame gate on the transition to Subscribed; the live delta competes.</summary>
        [Fact]
        public async Task LiveDeltaDecodedAfterRecoveredBase()
        {
            const string delta = "B\n6@0,5:therexz4NK;";
            Assert.Equal("hello there", Encoding.UTF8.GetString(
                Fossil.ApplyDelta(Encoding.UTF8.GetBytes("hello world"), Encoding.UTF8.GetBytes(delta))));

            var held = HoldFirstSubscribe();
            var client = NewClient();
            TrackFrameThread(client);
            client.Connect();
            await client.ReadyAsync(Wait);

            var liveSeen = new ManualResetEventSlim();
            var live = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var sub = client.NewSubscription("delta", new CentrifugeSubscriptionOptions { Delta = "fossil" });
            sub.StateChanged += (_, e) =>
            {
                if (e.NewState == CentrifugeSubscriptionState.Subscribed) FrameGate(liveSeen);
            };
            sub.Publication += (_, e) =>
            {
                if (e.Offset != 2) return;
                liveSeen.Set();
                live.TrySetResult(Encoding.UTF8.GetString(e.Data.Span));
            };
            sub.Subscribe();

            var cmd = await held.Task.WaitAsync(Wait);
            await _server.SendRepliesAsync(
                FrameMarker(),
                new Reply
                {
                    Id = cmd.Id,
                    Subscribe = new SubscribeResult
                    {
                        Recoverable = true, Positioned = true, Epoch = "e", Offset = 1, Recovered = true, Delta = true,
                        Publications = { Pub(1, "hello world") },
                    },
                },
                new Reply { Push = new Push { Channel = "delta", Pub = DeltaPub(2, delta) } });

            Assert.Equal("hello there", await live.Task.WaitAsync(Wait));
        }

        /// <summary>
        /// Delta subscription: only a publication flagged as a delta is decoded against the previous
        /// data; a recovered one that fails to decode raises Error, ends the delivery and disconnects
        /// the client with BadProtocol.
        /// </summary>
        [Fact]
        public async Task RecoveredDeltaDecodesFlaggedOnlyAndReportsCorruption()
        {
            var held = HoldFirstSubscribe();
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);
            var disconnected = new TaskCompletionSource<CentrifugeDisconnectedEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.Disconnected += (_, e) => disconnected.TrySetResult(e);

            var sub = client.NewSubscription("delta-corrupt", new CentrifugeSubscriptionOptions { Delta = "fossil" });
            var error = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            sub.Publication += (_, e) => Record(Encoding.UTF8.GetString(e.Data.Span));
            sub.Error += (_, e) =>
            {
                Record("error:" + e.Type);
                error.TrySetResult();
            };
            sub.Subscribe();

            var cmd = await held.Task.WaitAsync(Wait);
            await _server.SendRepliesAsync(new Reply
            {
                Id = cmd.Id,
                Subscribe = new SubscribeResult
                {
                    Recoverable = true, Positioned = true, Epoch = "e", Offset = 3, Recovered = true, Delta = true,
                    Publications = { Pub(1, "hello world"), Pub(2, "plain"), DeltaPub(3, "not a delta") },
                },
            });

            await error.Task.WaitAsync(Wait);
            Assert.Equal(CentrifugeDisconnectedCodes.BadProtocol, (await disconnected.Task.WaitAsync(Wait)).Code);
            Assert.Equal(new[] { "hello world", "plain", "error:publicationDecode" }, Recorded());
        }

        /// <summary>
        /// Channel compaction: a push carrying the numeric id assigned by the subscribe
        /// reply, in the same frame as the reply, must be routed. No gate: before the
        /// reply is applied the id is unknown and the push is dropped, which happens
        /// often enough to fail within the iterations.
        /// </summary>
        [Fact]
        public async Task CompactedPushInSameFrameAsReplyIsNotDropped()
        {
            _server.OnCommandFrame = cmd => cmd.Subscribe == null ? null : new[]
            {
                new Reply { Id = cmd.Id, Subscribe = new SubscribeResult { Id = 42 } },
                new Reply { Push = new Push { Id = 42, Pub = Pub(0, "x") } },
            };
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var sub = client.NewSubscription("compacted");
            var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            sub.Publication += (_, _) => Volatile.Read(ref received).TrySetResult();

            const int iterations = 20;
            for (var i = 0; i < iterations; i++)
            {
                Volatile.Write(ref received, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
                var unsubscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                EventHandler<CentrifugeUnsubscribedEventArgs> onUnsubscribed = (_, _) => unsubscribed.TrySetResult();
                sub.Subscribe();
                await Volatile.Read(ref received).Task.WaitAsync(Wait);
                sub.Unsubscribed += onUnsubscribed;
                sub.Unsubscribe();
                await unsubscribed.Task.WaitAsync(Wait);
                sub.Unsubscribed -= onUnsubscribed;
            }
        }

        /// <summary>
        /// A live publication that follows the subscribe reply advances the recovery
        /// position; the reply must not roll it back when applied late. No gate: each
        /// reply carries an even offset and is followed by the next (odd) publication,
        /// so every resubscribe must recover from an odd offset. A message closing the
        /// frame is the barrier: the receive loop processes it only after the publication.
        /// </summary>
        [Fact]
        public async Task StreamPositionNotRolledBackBySameFrameReply()
        {
            ulong next = 10;
            _server.OnCommandFrame = cmd =>
            {
                if (cmd.Subscribe == null) return null;
                var offset = next;
                next += 2;
                return new[]
                {
                    new Reply
                    {
                        Id = cmd.Id,
                        Subscribe = new SubscribeResult
                        {
                            Recoverable = true, Positioned = true, Epoch = "e", Offset = offset, WasRecovering = true, Recovered = true,
                        },
                    },
                    new Reply { Push = new Push { Channel = "position", Pub = Pub(offset + 1, "live") } },
                    new Reply { Push = new Push { Message = new Message { Data = ByteString.CopyFromUtf8("barrier") } } },
                };
            };
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var sub = client.NewSubscription("position", new CentrifugeSubscriptionOptions
            {
                Since = new CentrifugeStreamPosition(5, "e"),
            });
            var barrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            client.Message += (_, _) => Volatile.Read(ref barrier).TrySetResult();

            const int iterations = 30;
            for (var i = 0; i < iterations; i++)
            {
                Volatile.Write(ref barrier, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
                var unsubscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                EventHandler<CentrifugeUnsubscribedEventArgs> onUnsubscribed = (_, _) => unsubscribed.TrySetResult();
                sub.Subscribe();
                await Volatile.Read(ref barrier).Task.WaitAsync(Wait);
                sub.Unsubscribed += onUnsubscribed;
                sub.Unsubscribe();
                await unsubscribed.Task.WaitAsync(Wait);
                sub.Unsubscribed -= onUnsubscribed;
            }

            var offsets = _server.Received.Where(c => c.Subscribe != null).Skip(1).Select(c => c.Subscribe.Offset).ToList();
            var rolledBack = offsets.Where(o => o % 2 == 0).ToList();
            Assert.True(rolledBack.Count == 0,
                $"position rolled back in {rolledBack.Count}/{offsets.Count} resubscribes: {string.Join(",", offsets)}");
        }

        /// <summary>
        /// A publication handler of an ended session returning while the next session's subscribe reply
        /// is applied (released from its StateChanged) advances only its own base: the reply's base is
        /// where the next resubscribe recovers from.
        /// </summary>
        [Fact]
        public async Task LateDeliveryOfEndedSessionDoesNotBlockNewBase()
        {
            var subscribes = 0;
            _server.OnCommand = cmd => cmd.Subscribe == null ? null : new Reply
            {
                Id = cmd.Id,
                Subscribe = Interlocked.Increment(ref subscribes) == 1
                    ? new SubscribeResult { Recoverable = true, Positioned = true, Epoch = "old", Offset = 4 }
                    : new SubscribeResult { Recoverable = true, Positioned = true, Epoch = "new", Offset = 10 },
            };
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);
            var sub = client.NewSubscription("late-delivery", new CentrifugeSubscriptionOptions { Recoverable = true });
            sub.Subscribe();
            await sub.ReadyAsync(Wait);

            var delivering = new ManualResetEventSlim();
            var release = new ManualResetEventSlim();
            var resubscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            sub.Publication += (_, _) =>
            {
                delivering.Set();
                release.Wait(Wait);
            };
            sub.StateChanged += (_, e) =>
            {
                if (e.NewState != CentrifugeSubscriptionState.Subscribed || Volatile.Read(ref subscribes) != 2) return;
                release.Set();
                SpinWait.SpinUntil(() => ((CentrifugeStreamPosition?)Field(sub, "_streamPosition"))?.Offset == 5, Wait);
            };
            sub.Subscribed += (_, _) =>
            {
                if (Volatile.Read(ref subscribes) == 2) resubscribed.TrySetResult();
            };
            await _server.SendPushAsync(new Push { Channel = "late-delivery", Pub = Pub(5, "old") });
            Assert.True(delivering.Wait(Wait));
            NoPing.Fire(client);
            await resubscribed.Task.WaitAsync(Wait);

            NoPing.Fire(client);
            await Until(() => _server.Received.Count(c => c.Subscribe != null) == 3);
            var resubscribe = _server.Received.Last(c => c.Subscribe != null).Subscribe;
            Assert.Equal("new", resubscribe.Epoch);
            Assert.Equal(10UL, resubscribe.Offset);
        }

        /// <summary>A channel without a stream at subscribe gets its epoch with the first publication: the
        /// resubscribe recovers from it (as centrifuge-js).</summary>
        [Fact]
        public async Task PublicationEpochBecomesRecoveryEpoch()
        {
            _server.OnCommand = cmd => cmd.Subscribe == null ? null : new Reply
            {
                Id = cmd.Id,
                Subscribe = new SubscribeResult { Recoverable = true, Positioned = true, Epoch = "", Offset = 0 },
            };
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);
            var sub = client.NewSubscription("late-stream", new CentrifugeSubscriptionOptions { Recoverable = true });
            var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            sub.Publication += (_, _) => delivered.TrySetResult();
            sub.Subscribe();
            await sub.ReadyAsync(Wait);

            await _server.SendPushAsync(new Push { Channel = "late-stream", Pub = new Publication { Offset = 1, Epoch = "E1", Data = ByteString.CopyFromUtf8("first") } });
            await delivered.Task.WaitAsync(Wait);
            NoPing.Fire(client);
            await Until(() => _server.Received.Count(c => c.Subscribe != null) == 2);

            var resubscribe = _server.Received.Last(c => c.Subscribe != null).Subscribe;
            Assert.True(resubscribe.Recover);
            Assert.Equal("E1", resubscribe.Epoch);
            Assert.Equal(1UL, resubscribe.Offset);
        }

        /// <summary>A resubscribe the publication handler starts recovers from past that publication: it
        /// isn't delivered twice (as centrifuge-js _pendingOffset).</summary>
        [Fact]
        public async Task ResubscribeFromPublicationHandlerRecoversPastIt()
        {
            _server.OnCommand = cmd => cmd.Subscribe == null ? null : new Reply
            {
                Id = cmd.Id,
                Subscribe = new SubscribeResult { Recoverable = true, Positioned = true, Epoch = "e", Offset = 5 },
            };
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);
            var sub = client.NewSubscription("resubscribe-in-handler", new CentrifugeSubscriptionOptions { Recoverable = true });
            sub.Subscribe();
            await sub.ReadyAsync(Wait);
            var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            sub.Publication += (_, _) =>
            {
                sub.Unsubscribe();
                sub.Subscribe();
                SpinWait.SpinUntil(() => _server.Received.Count(c => c.Subscribe != null) == 2, Wait);
                handled.TrySetResult();
            };

            await _server.SendPushAsync(new Push { Channel = "resubscribe-in-handler", Pub = Pub(6, "sixth") });
            await handled.Task.WaitAsync(Wait);

            var resubscribe = _server.Received.Last(c => c.Subscribe != null).Subscribe;
            Assert.True(resubscribe.Recover);
            Assert.Equal(6UL, resubscribe.Offset);
        }

        /// <summary>A positioned channel that isn't recoverable doesn't ask for recovery on resubscribe
        /// (as centrifuge-js).</summary>
        [Fact]
        public async Task PositionedOnlySubscriptionDoesNotRecover()
        {
            _server.OnCommand = cmd => cmd.Subscribe == null ? null : new Reply
            {
                Id = cmd.Id,
                Subscribe = new SubscribeResult { Positioned = true, Epoch = "e", Offset = 5 },
            };
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);
            var sub = client.NewSubscription("positioned", new CentrifugeSubscriptionOptions { Positioned = true });
            sub.Subscribe();
            await sub.ReadyAsync(Wait);

            NoPing.Fire(client);
            await Until(() => _server.Received.Count(c => c.Subscribe != null) == 2);

            Assert.False(_server.Received.Last(c => c.Subscribe != null).Subscribe.Recover);
        }

        /// <summary>
        /// The same for a server-side subscription: a late delivery of the ended session, released from
        /// the next Connected, doesn't keep the connect reply's base out of the registry.
        /// </summary>
        [Fact]
        public async Task LateServerSideDeliveryOfEndedSessionDoesNotBlockNewBase()
        {
            var connects = 0;
            _server.OnCommand = cmd => cmd.Connect == null ? null : new Reply
            {
                Id = cmd.Id,
                Connect = new ConnectResult
                {
                    Client = "fake-client",
                    Version = "0.0.0",
                    Subs =
                    {
                        ["srv"] = Interlocked.Increment(ref connects) == 1
                            ? new SubscribeResult { Recoverable = true, Positioned = true, Epoch = "old", Offset = 4 }
                            : new SubscribeResult { Recoverable = true, Positioned = true, Epoch = "new", Offset = 10 },
                    },
                },
            };
            var client = NewClient();
            var delivering = new ManualResetEventSlim();
            var release = new ManualResetEventSlim();
            var resubscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var registry = (System.Collections.Concurrent.ConcurrentDictionary<string, ServerSubscription>)Field(client, "_serverSubscriptions")!;
            client.Publication += (_, _) =>
            {
                delivering.Set();
                release.Wait(Wait);
            };
            client.Connected += (_, _) =>
            {
                if (Volatile.Read(ref connects) != 2) return;
                release.Set();
                SpinWait.SpinUntil(() => registry.TryGetValue("srv", out var entry) && entry.Offset == 5, Wait);
            };
            client.ServerSubscribed += (_, _) =>
            {
                if (Volatile.Read(ref connects) == 2) resubscribed.TrySetResult();
            };
            client.Connect();
            await client.ReadyAsync(Wait);
            await _server.SendPushAsync(new Push { Channel = "srv", Pub = Pub(5, "old") });
            Assert.True(delivering.Wait(Wait));
            NoPing.Fire(client);
            await resubscribed.Task.WaitAsync(Wait);

            NoPing.Fire(client);
            await Until(() => _server.Received.Count(c => c.Connect != null) == 3);
            var recover = _server.Received.Last(c => c.Connect != null).Connect.Subs["srv"];
            Assert.Equal("new", recover.Epoch);
            Assert.Equal(10UL, recover.Offset);
        }

        /// <summary>Frame gate on the first Connected; the live publication competes.</summary>
        [Fact]
        public async Task ServerSideRecoveredPublicationsPrecedeLiveAndPositionAdvances()
        {
            var firstConnect = new TaskCompletionSource<Command>(TaskCreationOptions.RunContinuationsAsynchronously);
            _server.OnCommand = cmd =>
                cmd.Connect != null && firstConnect.TrySetResult(cmd) ? FakeCentrifugoServer.NoReply : null;

            var client = NewClient();
            TrackFrameThread(client);
            var liveSeen = new ManualResetEventSlim();
            var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var connected = 0;
            client.Connected += (_, _) =>
            {
                Record("connected");
                if (Interlocked.Increment(ref connected) == 1) FrameGate(liveSeen);
            };
            client.ServerSubscribed += (_, _) => Record("server-subscribed");
            client.Publication += (_, e) =>
            {
                Record("pub" + e.Offset);
                if (e.Offset != 3) return;
                liveSeen.Set();
                done.TrySetResult();
            };
            client.Connect();

            var cmd = await firstConnect.Task.WaitAsync(Wait);
            var result = new ConnectResult { Client = "c", Version = "0.0.0", Ping = 25 };
            result.Subs.Add("ss", new SubscribeResult
            {
                Recoverable = true, Positioned = true, Epoch = "e", Offset = 2, Recovered = true,
                Publications = { Pub(1, "r1"), Pub(2, "r2") },
            });
            await _server.SendRepliesAsync(
                FrameMarker(),
                new Reply { Id = cmd.Id, Connect = result },
                new Reply { Push = new Push { Channel = "ss", Pub = Pub(3, "live") } });
            await done.Task.WaitAsync(Wait);
            Assert.Equal(new[] { "connected", "server-subscribed", "pub1", "pub2", "pub3" }, Recorded().Take(5));

            _server.CloseConnection();
            await Until(() => _server.Received.Count(c => c.Connect != null) == 2);
            Assert.Equal(3UL, _server.Received.Last(c => c.Connect != null).Connect.Subs["ss"].Offset);
        }

        /// <summary>Publications of an unrecovered server-side subscription are not delivered.</summary>
        [Fact]
        public async Task ServerSideUnrecoveredPublicationsAreNotDelivered()
        {
            var connect = new TaskCompletionSource<Command>(TaskCreationOptions.RunContinuationsAsynchronously);
            _server.OnCommand = cmd =>
                cmd.Connect != null && connect.TrySetResult(cmd) ? FakeCentrifugoServer.NoReply : null;

            var client = NewClient();
            var live = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            client.ServerSubscribed += (_, e) => Record("server-subscribed:" + e.Recovered);
            client.Publication += (_, e) =>
            {
                Record("pub" + e.Offset);
                if (e.Offset == 3) live.TrySetResult();
            };
            client.Connect();

            var cmd = await connect.Task.WaitAsync(Wait);
            var result = new ConnectResult { Client = "c", Version = "0.0.0", Ping = 25 };
            result.Subs.Add("ss", new SubscribeResult
            {
                Recoverable = true, Positioned = true, Epoch = "e", Offset = 2,
                Publications = { Pub(1, "r1"), Pub(2, "r2") },
            });
            await _server.SendRepliesAsync(
                new Reply { Id = cmd.Id, Connect = result },
                new Reply { Push = new Push { Channel = "ss", Pub = Pub(3, "live") } });
            await live.Task.WaitAsync(Wait);
            Assert.Equal(new[] { "server-subscribed:False", "pub3" }, Recorded());
        }

        /// <summary>
        /// Frame gate on the first transition to Subscribed; the teardown by the Disconnect push
        /// competes, and with it the Subscribed of the resubscribe.
        /// </summary>
        [Fact]
        public async Task TeardownRightAfterReplyIsOrderedAfterSubscribed()
        {
            var held = HoldFirstSubscribe();
            var client = NewClient();
            TrackFrameThread(client);
            client.Connect();
            await client.ReadyAsync(Wait);

            var subscribedSeen = new ManualResetEventSlim();
            var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var gated = 0;
            var subscribedCount = 0;
            var sub = client.NewSubscription("teardown");
            sub.StateChanged += (_, e) =>
            {
                Record("state:" + e.NewState);
                if (e.NewState == CentrifugeSubscriptionState.Subscribed && Interlocked.Exchange(ref gated, 1) == 0)
                    FrameGate(subscribedSeen);
            };
            sub.Subscribing += (_, _) => Record("subscribing");
            sub.Subscribed += (_, _) =>
            {
                Record("subscribed");
                subscribedSeen.Set();
                if (Interlocked.Increment(ref subscribedCount) == 2) second.TrySetResult();
            };
            sub.Subscribe();

            var cmd = await held.Task.WaitAsync(Wait);
            Record("--reply--");
            await _server.SendRepliesAsync(
                FrameMarker(),
                new Reply { Id = cmd.Id, Subscribe = new SubscribeResult() },
                new Reply { Push = new Push { Disconnect = new Disconnect { Code = 3000, Reason = "go away", Reconnect = true } } });

            await second.Task.WaitAsync(Wait);
            Assert.Equal(new[]
            {
                "state:Subscribing", "subscribing", "--reply--",
                "state:Subscribed", "subscribed",
                "state:Subscribing", "subscribing",
                "state:Subscribed", "subscribed",
            }, Recorded());
        }

        [Fact]
        public async Task StaleErrorReplyDoesNotUnsubscribeNewAttempt()
        {
            var held = HoldFirstSubscribe();
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var unsubscribed = System.Threading.Channels.Channel.CreateUnbounded<CentrifugeUnsubscribedEventArgs>();
            var sub = client.NewSubscription("stale-error", WithoutBackoff());
            sub.Unsubscribed += (_, e) => unsubscribed.Writer.TryWrite(e);
            sub.Subscribe();

            var cmd = await held.Task.WaitAsync(Wait);
            sub.Unsubscribe();
            sub.Subscribe();
            Assert.Equal(CentrifugeUnsubscribedCodes.UnsubscribeCalled,
                (await unsubscribed.Reader.ReadAsync().AsTask().WaitAsync(Wait)).Code);

            await _server.SendReplyAsync(new Reply { Id = cmd.Id, Error = new Error { Code = 103, Message = "permission denied" } });

            await sub.ReadyAsync(Wait);
            Assert.False(unsubscribed.Reader.TryRead(out var extra), $"stale error unsubscribed the new attempt: {extra?.Code}");
        }

        /// <summary>
        /// A permanent subscribe error of a connection that is torn down before the
        /// error is applied must not unsubscribe: the subscription resubscribes on the
        /// next connection. The Error handler (raised before the error is applied) waits
        /// for the teardown; <see cref="Wait"/> only bounds it, the wait must not expire.
        /// </summary>
        [Fact]
        public async Task SubscribeErrorOfTornDownConnectionIsNotApplied()
        {
            var subscribes = 0;
            _server.OnCommand = cmd =>
            {
                if (cmd.Subscribe == null || Interlocked.Increment(ref subscribes) > 1) return null;
                return new Reply { Id = cmd.Id, Error = new Error { Code = 103, Message = "permission denied" } };
            };
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var gateEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var teardown = new ManualResetEventSlim();
            client.Connecting += (_, _) => teardown.Set();
            var unsubscribed = 0;
            var sub = client.NewSubscription("torn-down-error");
            sub.Error += (_, _) =>
            {
                if (gateEntered.TrySetResult()) teardown.Wait(Wait);
            };
            sub.Unsubscribed += (_, _) => Interlocked.Increment(ref unsubscribed);
            sub.Subscribe();

            await gateEntered.Task.WaitAsync(Wait);
            _ = Task.Run(() => NoPing.Fire(client));
            await sub.ReadyAsync(Wait);

            Assert.Equal(0, Volatile.Read(ref unsubscribed));
            Assert.Equal(2, _server.Received.Count(c => c.Connect != null));
        }

        /// <summary>
        /// The server gets subscribe#1, unsubscribe, then answers subscribe#1: the server keeps a
        /// subscription only if the new attempt sent its own subscribe after the unsubscribe.
        /// </summary>
        [Fact]
        public async Task SupersededSuccessReplyIsNotAdoptedByNewAttempt()
        {
            var held = HoldFirstSubscribe();
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var sub = client.NewSubscription("superseded", WithoutBackoff());
            sub.Subscribe();
            var cmd = await held.Task.WaitAsync(Wait);

            var unsubscribeSeen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var hold = _server.OnCommand!;
            _server.OnCommand = c =>
            {
                if (c.Unsubscribe != null) unsubscribeSeen.TrySetResult();
                return hold(c);
            };
            sub.Unsubscribe();
            sub.Subscribe();
            await unsubscribeSeen.Task.WaitAsync(Wait);
            await _server.SendReplyAsync(new Reply { Id = cmd.Id, Subscribe = new SubscribeResult() });

            await sub.ReadyAsync(Wait);
            var received = _server.Received.ToList();
            var unsubscribeIndex = received.FindIndex(c => c.Unsubscribe != null);
            Assert.Contains(received.Skip(unsubscribeIndex + 1), c => c.Subscribe != null);
        }

        [Fact]
        public async Task SupersededGetStateResultIsNotUsed()
        {
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var firstCall = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseFirst = new TaskCompletionSource<CentrifugeStreamPosition>(TaskCreationOptions.RunContinuationsAsynchronously);
            var calls = 0;
            var options = WithoutBackoff();
            options.GetState = _ =>
            {
                if (Interlocked.Increment(ref calls) != 1) return Task.FromResult(new CentrifugeStreamPosition(200, "second"));
                firstCall.TrySetResult();
                return releaseFirst.Task;
            };
            var sub = client.NewSubscription("get-state", options);
            sub.Subscribe();
            await firstCall.Task.WaitAsync(Wait);

            sub.Unsubscribe();
            sub.Subscribe();
            releaseFirst.TrySetResult(new CentrifugeStreamPosition(100, "first"));

            await sub.ReadyAsync(Wait);
            var last = _server.LastSubscribe()!;
            Assert.Equal("second", last.Epoch);
            Assert.Equal(200UL, last.Offset);
        }

        [Fact]
        public async Task SupersededGetTokenFailureHandsOverWithoutBackoff()
        {
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var firstCall = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseFirst = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var calls = 0;
            var options = WithoutBackoff();
            options.GetToken = _ =>
            {
                if (Interlocked.Increment(ref calls) != 1) return Task.FromResult("t");
                firstCall.TrySetResult();
                return releaseFirst.Task;
            };
            var sub = client.NewSubscription("get-token", options);
            sub.Error += (_, e) => Record("error:" + e.Type);
            sub.Subscribe();
            await firstCall.Task.WaitAsync(Wait);

            sub.Unsubscribe();
            sub.Subscribe();
            releaseFirst.TrySetException(new InvalidOperationException("token service down"));

            await sub.ReadyAsync(Wait);
            Assert.DoesNotContain(Recorded(), r => r.StartsWith("error:subscribe", StringComparison.Ordinal));
        }

        [Fact]
        public async Task SupersededGetStateFailureHandsOverWithoutError()
        {
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var firstCall = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseFirst = new TaskCompletionSource<CentrifugeStreamPosition>(TaskCreationOptions.RunContinuationsAsynchronously);
            var calls = 0;
            var options = WithoutBackoff();
            options.GetState = _ =>
            {
                if (Interlocked.Increment(ref calls) != 1) return Task.FromResult(new CentrifugeStreamPosition(200, "second"));
                firstCall.TrySetResult();
                return releaseFirst.Task;
            };
            var sub = client.NewSubscription("get-state-failure", options);
            sub.Error += (_, e) => Record("error:" + e.Type);
            sub.Subscribe();
            await firstCall.Task.WaitAsync(Wait);

            sub.Unsubscribe();
            sub.Subscribe();
            releaseFirst.TrySetException(new InvalidOperationException("state service down"));

            await sub.ReadyAsync(Wait);
            Assert.DoesNotContain("error:getState", Recorded());
        }

        /// <summary>
        /// The connection token a replaced connect attempt obtains must not be used on
        /// the new transport, and its "unauthorized" must not disconnect the client.
        /// The new attempt's connect is held so the client stays Connecting meanwhile.
        /// The stale GetToken is released inline, so its outcome has been applied when the
        /// release returns; a send once the new session is up is the barrier for any connect
        /// it queued on the new transport.
        /// </summary>
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task StaleConnectTokenIsNotUsedByNewTransport(bool unauthorized)
        {
            var held = new TaskCompletionSource<Command>(TaskCreationOptions.RunContinuationsAsynchronously);
            _server.OnCommand = cmd =>
                cmd.Connect != null && held.TrySetResult(cmd) ? FakeCentrifugoServer.NoReply : null;
            var releaseFirst = new TaskCompletionSource<string>();
            var calls = 0;
            var client = NewClient(new CentrifugeClientOptions
            {
                GetToken = () => Interlocked.Increment(ref calls) == 1 ? releaseFirst.Task : Task.FromResult("t"),
            });
            client.Connect();
            await UntilAwaited(releaseFirst.Task);

            client.Disconnect();
            client.Connect();
            var cmd = await held.Task.WaitAsync(Wait);
            await CompleteInline(() =>
            {
                if (unauthorized) releaseFirst.TrySetException(new CentrifugeUnauthorizedException());
                else releaseFirst.TrySetResult("stale");
            });

            Assert.Equal(CentrifugeClientState.Connecting, client.State);
            await _server.SendReplyAsync(new Reply { Id = cmd.Id, Connect = _server.ConnectResult });
            await client.ReadyAsync(Wait);
            await client.SendAsync(new byte[] { 1 });
            await Until(() => _server.Received.Any(c => c.Send != null));
            Assert.Equal(1, _server.Received.Count(c => c.Connect != null));
        }

        /// <summary>
        /// A permanent error reply to the token refresh of a previous subscribe session must not
        /// unsubscribe the current one. The test drives the refresh and awaits it, so its outcome
        /// has been applied.
        /// </summary>
        [Fact]
        public async Task StaleSubRefreshErrorDoesNotUnsubscribeNewSession()
        {
            var refresh = new TaskCompletionSource<Command>(TaskCreationOptions.RunContinuationsAsynchronously);
            _server.OnCommand = cmd =>
                cmd.SubRefresh != null && refresh.TrySetResult(cmd) ? FakeCentrifugoServer.NoReply : null;

            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var subscribed = System.Threading.Channels.Channel.CreateUnbounded<bool>();
            var unsubscribed = System.Threading.Channels.Channel.CreateUnbounded<CentrifugeUnsubscribedEventArgs>();
            var sub = client.NewSubscription("refresh", new CentrifugeSubscriptionOptions
            {
                Token = "t",
                GetToken = _ => Task.FromResult("t"),
            });
            sub.Subscribed += (_, _) => subscribed.Writer.TryWrite(true);
            sub.Unsubscribed += (_, e) => unsubscribed.Writer.TryWrite(e);
            sub.Error += (_, e) => Record("error:" + e.Type);
            sub.Subscribe();
            await subscribed.Reader.ReadAsync().AsTask().WaitAsync(Wait);

            var staleRefresh = sub.RefreshTokenAsync(sub.Epoch, client.ConnectionGeneration);
            var cmd = await refresh.Task.WaitAsync(Wait);
            sub.Unsubscribe();
            sub.Subscribe();
            await subscribed.Reader.ReadAsync().AsTask().WaitAsync(Wait);
            await unsubscribed.Reader.ReadAsync().AsTask().WaitAsync(Wait);

            await _server.SendReplyAsync(new Reply { Id = cmd.Id, Error = new Error { Code = 103, Message = "permission denied" } });
            await staleRefresh.WaitAsync(Wait);

            Assert.Equal(CentrifugeSubscriptionState.Subscribed, sub.State);
            Assert.False(unsubscribed.Reader.TryRead(out var extra), $"stale refresh error unsubscribed: {extra?.Code}");
            Assert.DoesNotContain("error:refresh", Recorded());
        }

        /// <summary>
        /// An empty token (unauthorized) from the refresh of a torn-down session doesn't disconnect
        /// the new one. The test drives the refresh and awaits it, so its outcome has been applied.
        /// </summary>
        [Fact]
        public async Task StaleConnectionRefreshTokenDoesNotDisconnectNewSession()
        {
            var releaseFirst = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var calls = 0;
            var connected = System.Threading.Channels.Channel.CreateUnbounded<bool>();
            var disconnected = 0;
            var client = NewClient(new CentrifugeClientOptions
            {
                Token = "t",
                GetToken = () => Interlocked.Increment(ref calls) == 1 ? releaseFirst.Task : Task.FromResult("t"),
            });
            client.Connected += (_, _) => connected.Writer.TryWrite(true);
            client.Disconnected += (_, _) => Interlocked.Increment(ref disconnected);
            client.Connect();
            await connected.Reader.ReadAsync().AsTask().WaitAsync(Wait);
            var staleRefresh = client.RefreshConnectionTokenAsync((int)Field(client, "_epoch")!);

            _server.CloseConnection();
            await connected.Reader.ReadAsync().AsTask().WaitAsync(Wait);
            releaseFirst.TrySetResult("");
            await staleRefresh.WaitAsync(Wait);

            Assert.Equal(1, Volatile.Read(ref calls));
            Assert.Equal(0, Volatile.Read(ref disconnected));
            Assert.Equal(CentrifugeClientState.Connected, client.State);
        }

        /// <summary>
        /// Nothing the transport receives after Disconnect() is dispatched: a connect reply
        /// must not flip the client back to Connected. The Disconnected handler runs before
        /// the transport is closed; it makes the server answer the held connect, followed
        /// by a message, and holds the teardown until the client's handler processed that frame.
        /// </summary>
        [Fact]
        public async Task ConnectReplyReceivedAfterDisconnectIsNotApplied()
        {
            var connect = new TaskCompletionSource<Command>(TaskCreationOptions.RunContinuationsAsynchronously);
            _server.OnCommand = cmd =>
                cmd.Connect != null && connect.TrySetResult(cmd) ? FakeCentrifugoServer.NoReply : null;

            var client = NewClient();
            var connectedEvents = 0;
            var messages = 0;
            client.Connected += (_, _) => Interlocked.Increment(ref connectedEvents);
            client.Message += (_, _) => Interlocked.Increment(ref messages);
            client.Connect();
            var cmd = await connect.Task.WaitAsync(Wait);

            var frameProcessed = FrameProcessed(client);
            var processed = false;
            client.Disconnected += (_, _) =>
            {
                _server.SendRepliesAsync(
                    new Reply { Id = cmd.Id, Connect = _server.ConnectResult },
                    new Reply { Push = new Push { Message = new Message { Data = ByteString.CopyFromUtf8("after") } } }).Wait();
                processed = frameProcessed.Wait(Wait);
            };
            client.Disconnect();

            Assert.True(processed, "the frame did not reach the transport before it closed");
            Assert.Equal(0, Volatile.Read(ref connectedEvents));
            Assert.Equal(0, Volatile.Read(ref messages));
            Assert.Equal(CentrifugeClientState.Disconnected, client.State);
        }

        /// <summary>
        /// The failure of a connect attempt whose transport was replaced (Disconnect()
        /// + Connect()) must not tear down the new transport. The replaced attempt is parked
        /// at its GetToken and fails once the new transport is connected; released inline, the
        /// failure has been handled when the release returns. A send is the barrier for a
        /// reconnect it started; it fails if the new transport was dropped.
        /// </summary>
        [Fact]
        public async Task StaleConnectFailureDoesNotTearDownNewTransport()
        {
            var releaseFirst = new TaskCompletionSource<string>();
            var calls = 0;
            var client = NewClient(new CentrifugeClientOptions
            {
                GetToken = () => Interlocked.Increment(ref calls) == 1 ? releaseFirst.Task : Task.FromResult("t"),
            });
            var connecting = System.Threading.Channels.Channel.CreateUnbounded<CentrifugeConnectingEventArgs>();
            client.Connect();
            await UntilAwaited(releaseFirst.Task);

            client.Connecting += (_, e) => connecting.Writer.TryWrite(e);
            client.Disconnect();
            client.Connect();
            await client.ReadyAsync(Wait);
            await CompleteInline(() => releaseFirst.TrySetException(
                new CentrifugeException(CentrifugeErrorCodes.ConnectionClosed, "connection closed", false)));
            await client.SendAsync(new byte[] { 1 });
            await Until(() => _server.Received.Any(c => c.Send != null));

            Assert.Equal(1, _server.Received.Count(c => c.Connect != null));
            Assert.Equal(CentrifugeClientState.Connected, client.State);
            Assert.Equal(CentrifugeConnectingCodes.ConnectCalled, (await connecting.Reader.ReadAsync().AsTask()).Code);
            Assert.False(connecting.Reader.TryRead(out var extra), $"unexpected reconnect: {extra?.Reason}");
        }

        /// <summary>
        /// While a teardown is in progress the client is Connecting but the old
        /// transport is still open. A subscribe triggered then (here by a server
        /// "resubscribe" unsubscribe) must not be sent and applied on the dying
        /// transport, or the subscription ends up Subscribed with no server-side
        /// counterpart on the next connection. The Connecting handler holds the teardown
        /// until the client's handler processed the push: it must not reach the subscription
        /// (its own teardown events follow the handler).
        /// </summary>
        [Fact]
        public async Task SubscribeDuringTeardownIsNotAppliedOnDyingTransport()
        {
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var sub = client.NewSubscription("teardown-window", new CentrifugeSubscriptionOptions
            {
                MinResubscribeDelay = TimeSpan.FromMilliseconds(10),
                MaxResubscribeDelay = TimeSpan.FromMilliseconds(50),
            });
            var subscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            sub.Subscribed += (_, _) => subscribed.TrySetResult();
            sub.Subscribe();
            await subscribed.Task.WaitAsync(Wait);
            sub.StateChanged += (_, e) => Record("state:" + e.NewState);
            sub.Subscribing += (_, _) => Record("subscribing");

            var pushProcessed = FrameProcessed(client);
            string[]? duringTeardown = null;
            var gated = 0;
            client.Connecting += (_, _) =>
            {
                if (Interlocked.Exchange(ref gated, 1) != 0) return;
                _server.SendPushAsync(new Push { Channel = "teardown-window", Unsubscribe = new Unsubscribe { Code = 2500, Reason = "resubscribe" } }).Wait();
                if (pushProcessed.Wait(Wait)) duringTeardown = Recorded();
            };
            NoPing.Fire(client);

            Assert.Equal(Array.Empty<string>(), duringTeardown);
            await client.ReadyAsync(Wait);
            await Until(() =>
            {
                var received = _server.Received.ToList();
                var lastConnect = received.FindLastIndex(c => c.Connect != null);
                return received.Skip(lastConnect + 1).Any(c => c.Subscribe != null);
            });
            Assert.Equal(2, _server.Received.Count(c => c.Connect != null));
        }

        /// <summary>
        /// A server unsubscribe (code below 2500) ends the subscription on the server: no unsubscribe
        /// command is sent back. A send issued after Unsubscribed reaches the server after any command
        /// queued by the transition, so its arrival bounds the check.
        /// </summary>
        [Fact]
        public async Task ServerUnsubscribeSendsNoUnsubscribeCommand()
        {
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var sub = client.NewSubscription("server-unsubscribe");
            var unsubscribed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            sub.Unsubscribed += (_, _) => unsubscribed.TrySetResult(true);
            sub.Subscribe();
            await sub.ReadyAsync(Wait);

            await _server.SendPushAsync(new Push { Channel = "server-unsubscribe", Unsubscribe = new Unsubscribe { Code = 2000, Reason = "server" } });
            await unsubscribed.Task.WaitAsync(Wait);
            await client.SendAsync(new byte[] { 1 });
            await Until(() => _server.Received.Any(c => c.Send != null));

            Assert.DoesNotContain(_server.Received, c => c.Unsubscribe != null);
        }

        /// <summary>
        /// A synchronous Dispose() from a handler raised on the receive loop must not
        /// wait for that loop to exit.
        /// </summary>
        [Theory]
        [InlineData("publication")]
        [InlineData("subscribed")]
        [InlineData("transport-error")]
        [InlineData("connecting")]
        public async Task SynchronousDisposeFromHandlerDoesNotDeadlock(string handler)
        {
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var disposed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var once = 0;
            void DisposeHere()
            {
                if (Interlocked.Exchange(ref once, 1) != 0) return;
                client.Dispose();
                disposed.TrySetResult();
            }

            var sub = client.NewSubscription("dispose");
            if (handler == "subscribed")
            {
                sub.Subscribed += (_, _) => DisposeHere();
                sub.Subscribe();
            }
            else
            {
                sub.Subscribe();
                await sub.ReadyAsync(Wait);
                switch (handler)
                {
                    case "publication":
                        sub.Publication += (_, _) => DisposeHere();
                        await _server.PublishChannelAsync("dispose", Encoding.UTF8.GetBytes("x"));
                        break;
                    case "transport-error":
                        client.Error += (_, e) => { if (e.Type == "transport") DisposeHere(); };
                        _server.CloseConnection();
                        break;
                    case "connecting":
                        client.Connecting += (_, _) => DisposeHere();
                        _server.CloseConnection();
                        break;
                }
            }

            await disposed.Task.WaitAsync(Wait);
            Assert.Equal(CentrifugeClientState.Disconnected, client.State);
        }

        [Fact]
        public async Task ThrowingConnectedHandlerDoesNotStrandSubscriptions()
        {
            var client = NewClient();
            var sub = client.NewSubscription("after-throw");
            sub.Subscribe();
            var errors = 0;
            client.Error += (_, _) => Interlocked.Increment(ref errors);
            client.Connected += (_, _) => throw new InvalidOperationException("handler bug");
            client.Connect();

            await sub.ReadyAsync(Wait);
            Assert.True(Volatile.Read(ref errors) > 0);
        }

        /// <summary>
        /// A throwing Connected handler must not keep the server-side subscriptions of the
        /// connect reply from being applied: the next connect recovers them from the
        /// reply's positions and no longer asks for a channel the reply dropped.
        /// </summary>
        [Fact]
        public async Task ThrowingConnectedHandlerStillAppliesServerSubscriptions()
        {
            var first = new ConnectResult { Client = "c", Version = "0.0.0", Ping = 25 };
            first.Subs.Add("gone", new SubscribeResult { Recoverable = true, Positioned = true, Epoch = "e", Offset = 1 });
            var second = new ConnectResult { Client = "c", Version = "0.0.0", Ping = 25 };
            second.Subs.Add("kept", new SubscribeResult { Recoverable = true, Positioned = true, Epoch = "e", Offset = 5 });
            _server.ConnectResult = first;

            var client = NewClient();
            var connected = 0;
            client.Connected += (_, _) =>
            {
                if (Interlocked.Increment(ref connected) == 2) throw new InvalidOperationException("handler bug");
            };
            client.Connect();
            await client.ReadyAsync(Wait);

            _server.ConnectResult = second;
            _server.CloseConnection();
            await Until(() => Volatile.Read(ref connected) == 2);
            _server.ConnectResult = new ConnectResult { Client = "c", Version = "0.0.0", Ping = 25 };
            _server.CloseConnection();
            await Until(() => _server.Received.Count(c => c.Connect != null) == 3);

            var subs = _server.Received.Last(c => c.Connect != null).Connect.Subs;
            Assert.Equal(new[] { "kept" }, subs.Keys.ToArray());
            Assert.Equal(5UL, subs["kept"].Offset);
        }

        /// <summary>
        /// A reply received in the same frame as an earlier reply whose handlers take longer
        /// than the command timeout must not time out: the receive loop claims every reply
        /// of a frame before dispatching any of it. The first reply's Publication handler holds
        /// the receive loop (<see cref="TimeoutHold"/>) past the second command's timer.
        /// </summary>
        [Fact]
        public async Task ReplyBehindSlowHandlerInSameFrameDoesNotTimeOut()
        {
            var held = new List<Command>();
            var both = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _server.OnCommand = cmd =>
            {
                if (cmd.Subscribe == null) return null;
                lock (held)
                {
                    if (held.Count == 2) return null;
                    held.Add(cmd);
                    if (held.Count == 2) both.TrySetResult();
                }
                return FakeCentrifugoServer.NoReply;
            };
            var client = NewClient(new CentrifugeClientOptions { Timeout = CommandTimeout });
            client.Connect();
            await client.ReadyAsync(Wait);

            var connecting = 0;
            var errors = 0;
            client.Connecting += (_, _) => Interlocked.Increment(ref connecting);
            var a = client.NewSubscription("a");
            var b = client.NewSubscription("b");
            var hold = new TimeoutHold();
            a.Publication += (_, _) => hold.Hold();
            b.Error += (_, _) => Interlocked.Increment(ref errors);
            a.Subscribe();
            b.Subscribe();
            await both.Task.WaitAsync(Wait);
            hold.Armed();

            Command ca, cb;
            lock (held)
            {
                ca = held.Single(c => c.Subscribe.Channel == "a");
                cb = held.Single(c => c.Subscribe.Channel == "b");
            }
            try
            {
                await _server.SendRepliesAsync(
                    new Reply
                    {
                        Id = ca.Id,
                        Subscribe = new SubscribeResult
                        {
                            Recoverable = true, Positioned = true, Epoch = "e", Offset = 1, Recovered = true,
                            Publications = { Pub(1, "r1") },
                        },
                    },
                    new Reply { Id = cb.Id, Subscribe = new SubscribeResult() });
                await hold.PastTimeoutAsync();
            }
            finally
            {
                hold.Release();
            }

            await b.ReadyAsync(Wait);
            Assert.Equal(0, Volatile.Read(ref errors));
            Assert.Equal(0, Volatile.Read(ref connecting));
        }

        /// <summary>
        /// The receive loop claims a reply before it is applied, while the command timer
        /// runs; a Subscribed handler slower than the timeout must not make the applied
        /// reply time out and force a reconnect. The handler holds the receive loop
        /// (<see cref="TimeoutHold"/>) past the command's timer.
        /// </summary>
        [Fact]
        public async Task SlowSubscribedHandlerDoesNotTimeOutAppliedReply()
        {
            var held = HoldFirstSubscribe();
            var client = NewClient(new CentrifugeClientOptions { Timeout = CommandTimeout });
            client.Connect();
            await client.ReadyAsync(Wait);

            var errors = 0;
            var connecting = 0;
            var subscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            client.Connecting += (_, _) => Interlocked.Increment(ref connecting);
            var sub = client.NewSubscription("slow-subscribed");
            var hold = new TimeoutHold();
            sub.Error += (_, _) => Interlocked.Increment(ref errors);
            sub.Subscribed += (_, _) =>
            {
                hold.Hold();
                subscribed.TrySetResult();
            };
            sub.Subscribe();
            var cmd = await held.Task.WaitAsync(Wait);
            hold.Armed();

            try
            {
                await _server.SendReplyAsync(new Reply { Id = cmd.Id, Subscribe = new SubscribeResult() });
                await hold.PastTimeoutAsync();
            }
            finally
            {
                hold.Release();
            }

            await subscribed.Task.WaitAsync(Wait);
            Assert.Equal(0, Volatile.Read(ref errors));
            Assert.Equal(0, Volatile.Read(ref connecting));
        }

        /// <summary>
        /// Recovered publications stop once a handler unsubscribes. The message closing the
        /// frame is the barrier: the receive loop processes it after the whole reply.
        /// </summary>
        [Fact]
        public async Task RecoveredDeliveryStopsWhenHandlerUnsubscribes()
        {
            _server.OnCommandFrame = cmd => cmd.Subscribe == null ? null : new[]
            {
                new Reply
                {
                    Id = cmd.Id,
                    Subscribe = new SubscribeResult
                    {
                        Recoverable = true, Positioned = true, Epoch = "e", Offset = 2, Recovered = true,
                        Publications = { Pub(1, "r1"), Pub(2, "r2") },
                    },
                },
                new Reply { Push = new Push { Message = new Message { Data = ByteString.CopyFromUtf8("barrier") } } },
            };
            var client = NewClient();
            var barrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            client.Message += (_, _) => barrier.TrySetResult();
            client.Connect();
            await client.ReadyAsync(Wait);

            var sub = client.NewSubscription("recover-stop");
            sub.Publication += (_, e) =>
            {
                Record("pub" + e.Offset);
                if (e.Offset == 1) sub.Unsubscribe();
            };
            sub.Subscribe();

            await barrier.Task.WaitAsync(Wait);
            Assert.Equal(new[] { "pub1" }, Recorded());
        }

        /// <summary>
        /// A recovery cut short by a handler (here: disconnecting on the first recovered
        /// publication) leaves the position at the last delivered publication, so the next
        /// recovery delivers the rest instead of skipping it.
        /// </summary>
        [Fact]
        public async Task RecoveryCutShortByHandlerResumesFromLastDelivered()
        {
            _server.OnSubscribe = (_, req) => req.Recover
                ? new SubscribeResult
                {
                    Recoverable = true, Positioned = true, Epoch = "e", Offset = 3, Recovered = true,
                    Publications = { Pub(1, "r1"), Pub(2, "r2"), Pub(3, "r3") },
                }
                : new SubscribeResult { Recoverable = true, Positioned = true, Epoch = "e" };
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var sub = client.NewSubscription("recover-resume");
            var cut = 0;
            sub.Publication += (_, e) =>
            {
                if (e.Offset == 1 && Interlocked.Increment(ref cut) == 1) client.Disconnect();
            };
            sub.Subscribe();
            await sub.ReadyAsync(Wait);

            _server.CloseConnection();
            await Until(() => client.State == CentrifugeClientState.Disconnected);
            client.Connect();
            await Until(() => _server.Received.Count(c => c.Subscribe != null) == 3);

            var resumed = _server.LastSubscribe()!;
            Assert.True(resumed.Recover);
            Assert.Equal(1UL, resumed.Offset);
        }

        /// <summary>
        /// Same for a server-side subscription: the next connect recovers it from the last
        /// publication delivered before the handler disconnected.
        /// </summary>
        [Fact]
        public async Task ServerSideRecoveryCutShortByHandlerResumesFromLastDelivered()
        {
            var connects = 0;
            _server.OnCommand = cmd =>
            {
                if (cmd.Connect == null) return null;
                var result = new ConnectResult { Client = "c", Version = "0.0.0", Ping = 25 };
                result.Subs.Add("ss", Interlocked.Increment(ref connects) == 2
                    ? new SubscribeResult
                    {
                        Recoverable = true, Positioned = true, Epoch = "e", Offset = 3, Recovered = true,
                        Publications = { Pub(1, "r1"), Pub(2, "r2"), Pub(3, "r3") },
                    }
                    : new SubscribeResult { Recoverable = true, Positioned = true, Epoch = "e" });
                return new Reply { Id = cmd.Id, Connect = result };
            };
            var client = NewClient();
            var cut = 0;
            client.Publication += (_, e) =>
            {
                if (e.Offset == 1 && Interlocked.Increment(ref cut) == 1) client.Disconnect();
            };
            client.Connect();
            await client.ReadyAsync(Wait);

            _server.CloseConnection();
            await Until(() => client.State == CentrifugeClientState.Disconnected);
            client.Connect();
            await Until(() => _server.Received.Count(c => c.Connect != null) == 3);

            Assert.Equal(1UL, _server.Received.Last(c => c.Connect != null).Connect.Subs["ss"].Offset);
        }

        /// <summary>
        /// A flush stuck sending on a torn-down transport (its session's flush lock held)
        /// must not hold up the commands of the next session.
        /// </summary>
        [Fact]
        public async Task FlushStuckOnTornDownSessionDoesNotBlockNextSession()
        {
            _server.OnCommand = cmd => cmd.Rpc != null ? new Reply { Id = cmd.Id, Rpc = new RPCResult() } : null;
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var flushLock = (SemaphoreSlim)typeof(CentrifugeClient)
                .GetField("_flushLock", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                .GetValue(client)!;
            await flushLock.WaitAsync();

            _server.CloseConnection();
            await Until(() => _server.Received.Count(c => c.Connect != null) == 2);
            await client.ReadyAsync(Wait);

            await client.RpcAsync("method", ReadOnlyMemory<byte>.Empty).WaitAsync(Wait);
        }

        /// <summary>
        /// A call whose reply the receive loop has claimed, but not yet dispatched behind a slow
        /// handler, stays cancellable by its caller past the command timeout. The Message handler
        /// opening the frame holds the receive loop (<see cref="TimeoutHold"/>) past the call's
        /// timer; the call is cancelled while held.
        /// </summary>
        [Fact]
        public async Task ClaimedCallStaysCancellablePastCommandTimeout()
        {
            var held = new TaskCompletionSource<Command>(TaskCreationOptions.RunContinuationsAsynchronously);
            _server.OnCommand = cmd => cmd.Rpc != null && held.TrySetResult(cmd) ? FakeCentrifugoServer.NoReply : null;
            var client = NewClient(new CentrifugeClientOptions { Timeout = CommandTimeout });
            client.Connect();
            await client.ReadyAsync(Wait);

            using var cts = new CancellationTokenSource();
            var hold = new TimeoutHold();
            client.Message += (_, _) => hold.Hold();
            var rpc = client.RpcAsync("method", ReadOnlyMemory<byte>.Empty, cts.Token);
            var cmd = await held.Task.WaitAsync(Wait);
            hold.Armed();

            try
            {
                await _server.SendRepliesAsync(
                    new Reply { Push = new Push { Message = new Message { Data = ByteString.CopyFromUtf8("m") } } },
                    new Reply { Id = cmd.Id, Rpc = new RPCResult() });
                await hold.PastTimeoutAsync();
                cts.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => rpc.WaitAsync(Wait));
            }
            finally
            {
                hold.Release();
            }
        }

        /// <summary>
        /// A failed recovery moves the position to the new top only once Subscribed reported the
        /// gap: when a handler unsubscribed before it, the next subscribe recovers from where it was.
        /// The first subscribe is awaited through its Subscribed event, not ReadyAsync (resolved
        /// before the reply's events): the handler added then sees only the resubscribe.
        /// </summary>
        [Fact]
        public async Task FailedRecoveryGapIsNotLostWhenSubscribedIsSkipped()
        {
            _server.OnSubscribe = (_, req) => new SubscribeResult
            {
                Recoverable = true, Positioned = true, Epoch = "e", Offset = req.Recover ? 50ul : 10ul,
            };
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);
            var sub = client.NewSubscription("gap");
            var subscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            sub.Subscribed += (_, _) => subscribed.TrySetResult();
            sub.Subscribe();
            await subscribed.Task.WaitAsync(Wait);

            var skip = 1;
            sub.StateChanged += (_, e) =>
            {
                if (e.NewState == CentrifugeSubscriptionState.Subscribed && Interlocked.Exchange(ref skip, 0) == 1)
                    sub.Unsubscribe();
            };
            _server.CloseConnection();
            await Until(() => Volatile.Read(ref skip) == 0 && sub.State == CentrifugeSubscriptionState.Unsubscribed);

            sub.Subscribe();
            await sub.ReadyAsync(Wait);
            Assert.Equal(10ul, _server.LastSubscribe()!.Offset);
        }

        /// <summary>
        /// A push routed by channel name doesn't reach a subscription a handler unsubscribed mid-frame:
        /// its position stays at the last delivered recovered publication, and the next subscribe
        /// recovers the rest.
        /// </summary>
        [Fact]
        public async Task PushAfterUnsubscribeInFrameDoesNotSkipRecoveredPublications()
        {
            var subscribes = 0;
            var held = new TaskCompletionSource<Command>(TaskCreationOptions.RunContinuationsAsynchronously);
            _server.OnCommand = cmd =>
                cmd.Subscribe != null && Interlocked.Increment(ref subscribes) == 2 && held.TrySetResult(cmd)
                    ? FakeCentrifugoServer.NoReply
                    : null;
            _server.OnSubscribe = (_, _) => new SubscribeResult { Recoverable = true, Positioned = true, Epoch = "e" };
            var client = NewClient();
            var frameDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            client.Message += (_, _) => frameDone.TrySetResult();
            client.Connect();
            await client.ReadyAsync(Wait);
            var sub = client.NewSubscription("cut");
            sub.Publication += (_, e) =>
            {
                if (e.Offset == 1) sub.Unsubscribe();
            };
            sub.Subscribe();
            await sub.ReadyAsync(Wait);

            _server.CloseConnection();
            var cmd = await held.Task.WaitAsync(Wait);
            await _server.SendRepliesAsync(
                new Reply
                {
                    Id = cmd.Id,
                    Subscribe = new SubscribeResult
                    {
                        Recoverable = true, Positioned = true, Epoch = "e", Offset = 2, Recovered = true,
                        Publications = { Pub(1, "p1"), Pub(2, "p2") },
                    },
                },
                new Reply { Push = new Push { Channel = "cut", Pub = Pub(3, "p3") } },
                new Reply { Push = new Push { Message = new Message { Data = ByteString.CopyFromUtf8("end") } } });
            await frameDone.Task.WaitAsync(Wait);

            sub.Subscribe();
            await sub.ReadyAsync(Wait);
            Assert.Equal(1ul, _server.LastSubscribe()!.Offset);
        }

        /// <summary>
        /// A throwing handler of a teardown transition can't stop the reconnect: the session comes
        /// back and the subscription resubscribes.
        /// </summary>
        [Fact]
        public async Task ThrowingSubscribingHandlerDoesNotStopReconnect()
        {
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);
            var sub = client.NewSubscription("reconnect");
            sub.Subscribe();
            await sub.ReadyAsync(Wait);
            sub.Subscribing += (_, _) => throw new InvalidOperationException("subscribing handler");

            _server.CloseConnection();
            await Until(() => _server.Received.Count(c => c.Connect != null) == 2);
            await sub.ReadyAsync(Wait);
        }

        /// <summary>
        /// A throwing Error handler can't break the SDK flow that reports an error: with Connected
        /// and Error both throwing, the session stays established and usable.
        /// </summary>
        [Fact]
        public async Task ThrowingErrorHandlerDoesNotBreakEstablishedSession()
        {
            _server.OnCommand = cmd => cmd.Rpc != null ? new Reply { Id = cmd.Id, Rpc = new RPCResult() } : null;
            var client = NewClient();
            client.Connected += (_, _) => throw new InvalidOperationException("connected handler");
            client.Error += (_, _) => throw new InvalidOperationException("error handler");
            client.Connect();
            await client.ReadyAsync(Wait);

            await client.RpcAsync("method", ReadOnlyMemory<byte>.Empty).WaitAsync(Wait);
            Assert.Equal(CentrifugeClientState.Connected, client.State);
        }

        /// <summary>
        /// A subscribe failed by the teardown of its connection is an outcome of an attempt that is
        /// no longer current: no Error, only the retry on the next session.
        /// </summary>
        [Fact]
        public async Task SubscribeFailedByTeardownRaisesNoError()
        {
            var held = HoldFirstSubscribe();
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var sub = client.NewSubscription("torn-down");
            sub.Error += (_, e) => Record("error:" + e.Type);
            sub.Subscribe();
            await held.Task.WaitAsync(Wait);

            _server.CloseConnection();
            await sub.ReadyAsync(Wait);
            Assert.DoesNotContain(Recorded(), r => r.StartsWith("error:subscribe", StringComparison.Ordinal));
        }

        /// <summary>An error reply to unsubscribe reconnects its session (as centrifuge-js).</summary>
        [Fact]
        public async Task UnsubscribeErrorReplyReconnectsSession()
        {
            _server.OnCommand = cmd => cmd.Unsubscribe != null
                ? new Reply { Id = cmd.Id, Error = new Error { Code = 100, Message = "internal" } }
                : null;
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);
            var sub = client.NewSubscription("unsub-error");
            sub.Subscribe();
            await sub.ReadyAsync(Wait);

            sub.Unsubscribe();

            await Until(() => _server.Received.Count(c => c.Connect != null) == 2);
        }

        /// <summary>A connect reply without its result fails the connect, which is retried.</summary>
        [Fact]
        public async Task ConnectReplyWithoutResultIsRetried()
        {
            var connects = 0;
            _server.OnCommand = cmd =>
                cmd.Connect != null && Interlocked.Increment(ref connects) == 1 ? new Reply { Id = cmd.Id } : null;
            var client = NewClient();
            Exception? error = null;
            client.Error += (_, e) => error ??= e.Exception;
            client.Connect();

            await client.ReadyAsync(Wait);
            Assert.Equal(2, connects);
            Assert.IsType<System.IO.InvalidDataException>(error);
        }

        /// <summary>A subscribe reply without its result fails the attempt, which is retried.</summary>
        [Fact]
        public async Task SubscribeReplyWithoutResultIsRetried()
        {
            var subscribes = 0;
            _server.OnCommand = cmd =>
                cmd.Subscribe != null && Interlocked.Increment(ref subscribes) == 1 ? new Reply { Id = cmd.Id } : null;
            var client = NewClient();
            client.Connect();
            var sub = client.NewSubscription("no-result", new CentrifugeSubscriptionOptions
            {
                MinResubscribeDelay = TimeSpan.FromMilliseconds(1),
                MaxResubscribeDelay = TimeSpan.FromMilliseconds(50),
            });
            Exception? error = null;
            sub.Error += (_, e) => error ??= e.Exception;
            sub.Subscribe();

            await sub.ReadyAsync(Wait);
            Assert.Equal(2, subscribes);
            Assert.IsType<System.IO.InvalidDataException>(error);
        }

        /// <summary>
        /// A server-side channel gone from the connect reply stays in the registry until its
        /// ServerUnsubscribed is raised: a handler disconnecting before that doesn't lose it.
        /// </summary>
        [Fact]
        public async Task ServerUnsubscribedIsNotLostWhenHandlerDisconnects()
        {
            var connects = 0;
            _server.OnCommand = cmd =>
            {
                if (cmd.Connect == null) return null;
                var result = new ConnectResult { Client = "c", Version = "0.0.0", Ping = 25 };
                result.Subs.Add("a", new SubscribeResult());
                if (Interlocked.Increment(ref connects) == 1) result.Subs.Add("b", new SubscribeResult());
                return new Reply { Id = cmd.Id, Connect = result };
            };
            var client = NewClient();
            var cut = 0;
            client.ServerSubscribed += (_, e) =>
            {
                if (Volatile.Read(ref connects) == 2 && Interlocked.Increment(ref cut) == 1) client.Disconnect();
            };
            client.ServerUnsubscribed += (_, e) => Record("unsubscribed:" + e.Channel);
            client.Connect();
            await client.ReadyAsync(Wait);

            _server.CloseConnection();
            await Until(() => client.State == CentrifugeClientState.Disconnected);
            client.Connect();

            await Until(() => Recorded().Contains("unsubscribed:b"));
        }

        /// <summary>
        /// A teardown fails the pending calls of its session at once: a throwing handler of the
        /// transition can't leave them hanging.
        /// </summary>
        [Fact]
        public async Task PendingCallFailsWhenTeardownHandlerThrows()
        {
            _server.OnCommand = cmd => cmd.Rpc != null ? FakeCentrifugoServer.NoReply : null;
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);
            client.Disconnected += (_, _) => throw new InvalidOperationException("handler");

            var rpc = client.RpcAsync("method", ReadOnlyMemory<byte>.Empty);
            await Until(() => _server.Received.Any(c => c.Rpc != null));
            client.Disconnect();

            var ex = await Assert.ThrowsAsync<CentrifugeException>(() => rpc.WaitAsync(Wait));
            Assert.Equal(CentrifugeErrorCodes.ConnectionClosed, ex.Code);
        }

        /// <summary>
        /// A message of a frame that fails to parse is reported at its place: an Error handler
        /// that disconnects doesn't lose the messages before it.
        /// </summary>
        [Fact]
        public async Task ParseErrorIsReportedAfterPrecedingMessages()
        {
            var client = NewClient();
            client.Message += (_, _) => Record("message");
            client.Error += (_, e) =>
            {
                if (e.Type != "parse") return;
                Record("parse");
                client.Disconnect();
            };
            client.Connect();
            await client.ReadyAsync(Wait);

            using var frame = new System.IO.MemoryStream();
            new Reply { Push = new Push { Message = new Message { Data = ByteString.CopyFromUtf8("m") } } }.WriteDelimitedTo(frame);
            frame.Write(new byte[] { 0x02, 0xFF, 0xFF }, 0, 3);
            await _server.SendRawAsync(frame.ToArray());

            await Until(() => Recorded().Contains("parse"));
            Assert.Equal(new[] { "message", "parse" }, Recorded());
        }

        /// <summary>
        /// A message of a frame that fails to parse reconnects the session: the messages after it
        /// aren't dispatched, so recovery resumes before the lost one.
        /// </summary>
        [Fact]
        public async Task ParseErrorReconnectsWithoutDispatchingRest()
        {
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);
            var messages = 0;
            client.Message += (_, _) => Interlocked.Increment(ref messages);
            var connecting = new TaskCompletionSource<CentrifugeConnectingEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.Connecting += (_, e) => connecting.TrySetResult(e);

            using var frame = new System.IO.MemoryStream();
            frame.Write(new byte[] { 0x02, 0xFF, 0xFF }, 0, 3);
            new Reply { Push = new Push { Message = new Message { Data = ByteString.CopyFromUtf8("m") } } }.WriteDelimitedTo(frame);
            await _server.SendRawAsync(frame.ToArray());

            var args = await connecting.Task.WaitAsync(Wait);
            Assert.Equal(CentrifugeConnectingCodes.TransportClosed, args.Code);
            Assert.Equal(0, Volatile.Read(ref messages));
        }

        /// <summary>The messages of a frame before a malformed one are still dispatched.</summary>
        [Fact]
        public async Task ReplyBeforeMalformedTailOfFrameIsApplied()
        {
            var connect = new TaskCompletionSource<Command>(TaskCreationOptions.RunContinuationsAsynchronously);
            _server.OnCommand = cmd =>
                cmd.Connect != null && connect.TrySetResult(cmd) ? FakeCentrifugoServer.NoReply : null;
            var client = NewClient();
            client.Connected += (_, e) => Record("connected:" + e.ClientId);
            client.Connect();
            var cmd = await connect.Task.WaitAsync(Wait);

            using var frame = new System.IO.MemoryStream();
            new Reply { Id = cmd.Id, Connect = new ConnectResult { Client = "first", Version = "0.0.0" } }.WriteDelimitedTo(frame);
            frame.Write(new byte[] { 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 }, 0, 6);
            await _server.SendRawAsync(frame.ToArray());

            await Until(() => Recorded().Length > 0);
            Assert.Equal("connected:first", Recorded()[0]);
        }

        /// <summary>
        /// An event handler that moves the subscription on (here: unsubscribes on the
        /// transition to Subscribed) must not be followed by the rest of the reply's events.
        /// The message closing the frame is the barrier.
        /// </summary>
        [Fact]
        public async Task SubscribedNotRaisedAfterStateChangedHandlerUnsubscribes()
        {
            _server.OnCommandFrame = cmd => cmd.Subscribe == null ? null : new[]
            {
                new Reply { Id = cmd.Id, Subscribe = new SubscribeResult() },
                new Reply { Push = new Push { Message = new Message { Data = ByteString.CopyFromUtf8("barrier") } } },
            };
            var client = NewClient();
            var barrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            client.Message += (_, _) => barrier.TrySetResult();
            client.Connect();
            await client.ReadyAsync(Wait);

            var sub = client.NewSubscription("unsubscribe-in-handler");
            sub.StateChanged += (_, e) =>
            {
                Record("state:" + e.NewState);
                if (e.NewState == CentrifugeSubscriptionState.Subscribed) sub.Unsubscribe();
            };
            sub.Subscribed += (_, _) => Record("subscribed");
            sub.Unsubscribed += (_, _) => Record("unsubscribed");
            sub.Subscribe();

            await barrier.Task.WaitAsync(Wait);
            Assert.Equal(new[] { "state:Subscribing", "state:Subscribed", "state:Unsubscribed", "unsubscribed" }, Recorded());
        }

        /// <summary>
        /// A first subscribe whose Subscribed a handler skipped (it unsubscribed on the transition)
        /// leaves no stream position: the next subscribe starts over instead of recovering from a
        /// base the app never received.
        /// </summary>
        [Fact]
        public async Task SkippedFirstSubscribedLeavesNoPositionToRecoverFrom()
        {
            _server.OnCommand = cmd => cmd.Subscribe == null ? null : new Reply
            {
                Id = cmd.Id,
                Subscribe = new SubscribeResult { Recoverable = true, Positioned = true, Epoch = "e", Offset = 10 },
            };
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var sub = client.NewSubscription("skipped-subscribed", new CentrifugeSubscriptionOptions { Recoverable = true });
            var first = true;
            sub.StateChanged += (_, e) =>
            {
                if (e.NewState != CentrifugeSubscriptionState.Subscribed || !first) return;
                first = false;
                sub.Unsubscribe();
            };
            sub.Subscribe();
            await Until(() => sub.State == CentrifugeSubscriptionState.Unsubscribed);
            sub.Subscribe();
            await sub.ReadyAsync(Wait);

            var subscribes = _server.Received.Where(c => c.Subscribe != null).ToList();
            Assert.Equal(2, subscribes.Count);
            Assert.False(subscribes[1].Subscribe.Recover);
        }

        /// <summary>
        /// The base Subscribed reports is recorded before the handler runs: a handler that
        /// reconnects at once resubscribes recovering from it.
        /// </summary>
        [Fact]
        public async Task ReconnectFromSubscribedHandlerRecoversFromReportedBase()
        {
            _server.OnCommand = cmd => cmd.Subscribe == null ? null : new Reply
            {
                Id = cmd.Id,
                Subscribe = new SubscribeResult { Recoverable = true, Positioned = true, Epoch = "e", Offset = 10 },
            };
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var sub = client.NewSubscription("reconnect-in-subscribed", new CentrifugeSubscriptionOptions { Recoverable = true });
            var first = true;
            sub.Subscribed += (_, _) =>
            {
                if (!first) return;
                first = false;
                client.Disconnect();
                client.Connect();
                SpinWait.SpinUntil(() => _server.Received.Count(c => c.Subscribe != null) == 2, Wait);
            };
            sub.Subscribe();
            await Until(() => _server.Received.Count(c => c.Subscribe != null) == 2);

            var resubscribe = _server.Received.Last(c => c.Subscribe != null).Subscribe;
            Assert.True(resubscribe.Recover);
            Assert.Equal(10UL, resubscribe.Offset);
            Assert.Equal("e", resubscribe.Epoch);
        }

        /// <summary>
        /// A token cancelled before the command is queued fails the call without sending it.
        /// A send queued after the calls failed is the barrier: what they queued arrives before it.
        /// </summary>
        [Fact]
        public async Task CancelledCommandIsNotSent()
        {
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            using var cancelled = new CancellationTokenSource();
            cancelled.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.SendAsync(new byte[] { 1 }, cancelled.Token));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.RpcAsync("m", new byte[] { 1 }, cancelled.Token));
            await client.SendAsync(new byte[] { 2 });
            await Until(() => _server.Received.Any(c => c.Send != null));

            Assert.DoesNotContain(_server.Received, c => c.Rpc != null);
            Assert.Equal(new byte[] { 2 }, Assert.Single(_server.Received, c => c.Send != null).Send.Data.ToByteArray());
        }

        /// <summary>
        /// A subscription token provider failure — even a timeout — is a token error retried with
        /// backoff, not a subscribe timeout: the client is not reconnected.
        /// </summary>
        [Fact]
        public async Task SubscribeTokenFailureIsRetriedAsTokenError()
        {
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);
            var calls = 0;
            var sub = client.NewSubscription("token-failure", new CentrifugeSubscriptionOptions
            {
                MinResubscribeDelay = TimeSpan.FromMilliseconds(1),
                MaxResubscribeDelay = TimeSpan.FromMilliseconds(50),
                GetToken = _ => Interlocked.Increment(ref calls) == 1
                    ? Task.FromException<string>(new CentrifugeTimeoutException())
                    : Task.FromResult("t"),
            });
            (int Code, string Type)? error = null;
            sub.Error += (_, e) => error ??= (e.Code, e.Type);
            sub.Subscribe();

            await sub.ReadyAsync(Wait);
            Assert.Equal((CentrifugeErrorCodes.SubscriptionSubscribeToken, "subscribeToken"), error);
            Assert.Equal(1, _server.Received.Count(c => c.Connect != null));
        }

        /// <summary>
        /// A connection token provider failure — even with a permanent server code — is a token error
        /// retried with backoff, not a server rejection of the connect.
        /// </summary>
        [Fact]
        public async Task ConnectTokenFailureIsRetriedAsTokenError()
        {
            var calls = 0;
            var client = NewClient(new CentrifugeClientOptions
            {
                GetToken = () => Interlocked.Increment(ref calls) == 1
                    ? Task.FromException<string>(new CentrifugeException(103, "permission denied", false))
                    : Task.FromResult("t"),
            });
            (int Code, string Type)? error = null;
            client.Error += (_, e) => error ??= (e.Code, e.Type);
            client.Connect();

            await client.ReadyAsync(Wait);
            Assert.Equal((CentrifugeErrorCodes.ClientConnectToken, "connectToken"), error);
        }

        /// <summary>
        /// An expired static token (109) that no GetToken can replace isn't retried: a configuration
        /// error, then Disconnected with Unauthorized (as centrifuge-js). A token set afterwards
        /// replaces it: the next connect sends it.
        /// </summary>
        [Fact]
        public async Task ExpiredStaticTokenWithoutGetTokenDisconnectsUnauthorized()
        {
            _server.OnCommand = cmd => cmd.Connect is { Token: "expired" }
                ? new Reply { Id = cmd.Id, Error = new Error { Code = 109, Message = "token expired" } }
                : null;
            var client = NewClient(new CentrifugeClientOptions { Token = "expired" });
            var errors = new System.Collections.Concurrent.ConcurrentQueue<string>();
            var disconnected = new TaskCompletionSource<CentrifugeDisconnectedEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.Error += (_, e) => errors.Enqueue(e.Type);
            client.Disconnected += (_, e) => disconnected.TrySetResult(e);
            client.Connect();

            Assert.Equal(CentrifugeDisconnectedCodes.Unauthorized, (await disconnected.Task.WaitAsync(Wait)).Code);
            Assert.Contains("configuration", errors);

            client.SetToken("fresh");
            client.Connect();
            await client.ReadyAsync(Wait);
            Assert.Equal("fresh", _server.Received.Last(c => c.Connect != null).Connect.Token);
        }

        /// <summary>A 109 for a token the app replaced while the connect was in flight doesn't mark the new
        /// one expired: the next connect sends it (as centrifuge-js).</summary>
        [Fact]
        public async Task ExpiredReplyForReplacedTokenKeepsNewToken()
        {
            var held = new TaskCompletionSource<Command>(TaskCreationOptions.RunContinuationsAsynchronously);
            _server.OnCommand = cmd => cmd.Connect is { Token: "old" } && held.TrySetResult(cmd) ? FakeCentrifugoServer.NoReply : null;
            var client = NewClient(new CentrifugeClientOptions { Token = "old" });
            client.Connect();
            var cmd = await held.Task.WaitAsync(Wait);

            client.SetToken("fresh");
            await _server.SendReplyAsync(new Reply { Id = cmd.Id, Error = new Error { Code = 109, Message = "token expired" } });

            await client.ReadyAsync(Wait);
            Assert.Equal("fresh", _server.Received.Last(c => c.Connect != null).Connect.Token);
        }

        /// <summary>
        /// A connect error 109 followed in the same frame by a Disconnect still marks the token for
        /// refresh — on the receive loop, before the Disconnect ends the attempt: the next connect
        /// gets a fresh token.
        /// </summary>
        [Fact]
        public async Task ExpiredTokenBeforeDisconnectInFrameIsRefreshed()
        {
            var held = new TaskCompletionSource<Command>(TaskCreationOptions.RunContinuationsAsynchronously);
            _server.OnCommand = cmd => cmd.Connect != null && held.TrySetResult(cmd) ? FakeCentrifugoServer.NoReply : null;
            var client = NewClient(new CentrifugeClientOptions
            {
                Token = "expired",
                GetToken = () => Task.FromResult("fresh"),
            });
            client.Connect();
            var cmd = await held.Task.WaitAsync(Wait);

            await _server.SendRepliesAsync(
                new Reply { Id = cmd.Id, Error = new Error { Code = 109, Message = "token expired" } },
                new Reply { Push = new Push { Disconnect = new Disconnect { Code = 3001, Reason = "shutdown" } } });

            await client.ReadyAsync(Wait);
            Assert.Equal("fresh", _server.Received.Last(c => c.Connect != null).Connect.Token);
        }

        /// <summary>Subscriptions sharing one options object keep their own tokens: the options aren't written.</summary>
        [Fact]
        public async Task SharedSubscriptionOptionsKeepTokensApart()
        {
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);
            var options = new CentrifugeSubscriptionOptions { GetToken = channel => Task.FromResult("token-" + channel) };

            var a = client.NewSubscription("a", options);
            a.Subscribe();
            await a.ReadyAsync(Wait);
            var b = client.NewSubscription("b", options);
            b.Subscribe();
            await b.ReadyAsync(Wait);

            Assert.Equal("token-b", _server.Received.Last(c => c.Subscribe?.Channel == "b").Subscribe.Token);
            Assert.Null(options.Token);
        }

        /// <summary>
        /// A permanent connect error disconnects with the server's code even when the server closes
        /// the connection right after it: the reply is applied on the receive loop before the close.
        /// </summary>
        [Fact]
        public async Task PermanentConnectErrorFollowedByCloseDisconnects()
        {
            var held = new TaskCompletionSource<Command>(TaskCreationOptions.RunContinuationsAsynchronously);
            _server.OnCommand = cmd => cmd.Connect != null && held.TrySetResult(cmd) ? FakeCentrifugoServer.NoReply : null;
            var client = NewClient();
            var disconnected = new TaskCompletionSource<CentrifugeDisconnectedEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.Disconnected += (_, e) => disconnected.TrySetResult(e);
            client.Connect();
            var cmd = await held.Task.WaitAsync(Wait);

            await _server.SendReplyAsync(new Reply { Id = cmd.Id, Error = new Error { Code = 103, Message = "permission denied" } });
            await _server.CloseConnectionGracefullyAsync();

            Assert.Equal(103, (await disconnected.Task.WaitAsync(Wait)).Code);
        }

        /// <summary>A transport close without a server code reconnects with TransportClosed, as centrifuge-js.</summary>
        [Fact]
        public async Task TransportCloseReconnectsWithTransportClosedCode()
        {
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);
            var connecting = new TaskCompletionSource<CentrifugeConnectingEventArgs>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.Connecting += (_, e) => connecting.TrySetResult(e);

            _server.CloseConnection();

            var args = await connecting.Task.WaitAsync(Wait);
            Assert.Equal(CentrifugeConnectingCodes.TransportClosed, args.Code);
            Assert.Equal("transport closed", args.Reason);
        }

        /// <summary>A message its session drops before the write fails its SendAsync (the flush is held).</summary>
        [Fact]
        public async Task SendDroppedByTeardownFails()
        {
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);
            var (flushLock, batch) = await HoldFlushAsync(client);

            var send = client.SendAsync(new byte[] { 1 });
            await Until(() => batch.Count == 1);
            client.Disconnect();

            var error = await Assert.ThrowsAsync<CentrifugeException>(() => send.WaitAsync(Wait));
            Assert.Equal(CentrifugeErrorCodes.ConnectionClosed, error.Code);
            flushLock.Release();
        }

        /// <summary>Cancelling a queued SendAsync ends the wait; the message is still written.</summary>
        [Fact]
        public async Task CancelledSendWaitStillWritesMessage()
        {
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);
            var (flushLock, batch) = await HoldFlushAsync(client);

            using var cancel = new CancellationTokenSource();
            var send = client.SendAsync(new byte[] { 1 }, cancel.Token);
            await Until(() => batch.Count == 1);
            cancel.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => send.WaitAsync(Wait));
            flushLock.Release();
            await Until(() => _server.Received.Any(c => c.Send != null));
        }

        /// <summary>Holds the flush of the client's session; returns its lock and the command queue.</summary>
        private static async Task<(SemaphoreSlim FlushLock, System.Collections.ICollection Batch)> HoldFlushAsync(CentrifugeClient client)
        {
            var flushLock = (SemaphoreSlim)typeof(CentrifugeClient)
                .GetField("_flushLock", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(client)!;
            var batch = (System.Collections.ICollection)typeof(CentrifugeClient)
                .GetField("_commandBatch", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(client)!;
            await flushLock.WaitAsync();
            return (flushLock, batch);
        }

        /// <summary>
        /// Same for the connect reply: once a handler disconnected, neither Connected nor the
        /// server-side subscription events of the reply follow Disconnected. A new server-side
        /// channel whose ServerSubscribed never reached the app is not recorded: the disconnect
        /// raises no ServerSubscribing for it and the next connect doesn't recover it. The events
        /// of the reply are complete once the client's handler processed its frame.
        /// </summary>
        [Theory]
        [InlineData("state")]
        [InlineData("connected")]
        public async Task ConnectReplyEventsStopAfterHandlerDisconnects(string handler)
        {
            var result = new ConnectResult { Client = "c", Version = "0.0.0", Ping = 25 };
            result.Subs.Add("ss", new SubscribeResult
            {
                Recoverable = true, Positioned = true, Epoch = "e", Offset = 1, Recovered = true,
                Publications = { Pub(1, "r1") },
            });
            _server.ConnectResult = result;
            var connect = new TaskCompletionSource<Command>(TaskCreationOptions.RunContinuationsAsynchronously);
            _server.OnCommand = cmd =>
                cmd.Connect != null && connect.TrySetResult(cmd) ? FakeCentrifugoServer.NoReply : null;

            var client = NewClient();
            client.StateChanged += (_, e) =>
            {
                if (handler == "state" && e.NewState == CentrifugeClientState.Connected) client.Disconnect();
            };
            client.Connected += (_, _) =>
            {
                Record("connected");
                if (handler == "connected") client.Disconnect();
            };
            client.Disconnected += (_, _) => Record("disconnected");
            client.ServerSubscribing += (_, e) => Record("server-subscribing:" + e.Channel);
            client.ServerSubscribed += (_, e) => Record("server-subscribed:" + e.Channel);
            client.Publication += (_, e) => Record("server-pub" + e.Offset);
            client.Connect();
            var cmd = await connect.Task.WaitAsync(Wait);

            var replyProcessed = FrameProcessed(client);
            await _server.SendReplyAsync(new Reply { Id = cmd.Id, Connect = result });
            await replyProcessed.WaitAsync(Wait);
            Assert.Equal(handler == "state"
                ? new[] { "disconnected" }
                : new[] { "connected", "disconnected" }, Recorded());

            handler = "none";
            client.Connect();
            await client.ReadyAsync(Wait);
            var reconnect = _server.Received.Last(c => c.Connect != null);
            Assert.False(reconnect.Connect.Subs.ContainsKey("ss"));
        }

        /// <summary>
        /// The server must receive subscribe and unsubscribe commands in the order of the
        /// transitions. Toggled while the first subscribe and unsubscribe are unanswered, the
        /// second unsubscribe must not wait for the first one's reply and reach the server
        /// after the subscribe of the latest attempt — the server would end unsubscribed
        /// while the client is Subscribed.
        /// </summary>
        [Fact]
        public async Task SubscribeAndUnsubscribeReachServerInTransitionOrder()
        {
            var heldSubscribe = new TaskCompletionSource<Command>(TaskCreationOptions.RunContinuationsAsynchronously);
            var heldUnsubscribe = new TaskCompletionSource<Command>(TaskCreationOptions.RunContinuationsAsynchronously);
            _server.OnCommand = cmd =>
            {
                if (cmd.Subscribe != null && heldSubscribe.TrySetResult(cmd)) return FakeCentrifugoServer.NoReply;
                if (cmd.Unsubscribe != null && heldUnsubscribe.TrySetResult(cmd)) return FakeCentrifugoServer.NoReply;
                return null;
            };
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var sub = client.NewSubscription("toggle", WithoutBackoff());
            sub.Subscribe();
            var subscribe = await heldSubscribe.Task.WaitAsync(Wait);
            sub.Unsubscribe();
            var unsubscribe = await heldUnsubscribe.Task.WaitAsync(Wait);
            sub.Subscribe();
            sub.Unsubscribe();
            sub.Subscribe();

            await _server.SendReplyAsync(new Reply { Id = subscribe.Id, Subscribe = new SubscribeResult() });
            await sub.ReadyAsync(Wait);
            await _server.SendReplyAsync(new Reply { Id = unsubscribe.Id, Unsubscribe = new UnsubscribeResult() });
            await Until(() => _server.Received.Count(c => c.Unsubscribe != null) == 2);

            var wire = _server.Received
                .Where(c => c.Subscribe != null || c.Unsubscribe != null)
                .Select(c => c.Subscribe != null ? "sub" : "unsub");
            Assert.Equal(new[] { "sub", "unsub", "unsub", "sub" }, wire);
            Assert.Equal(CentrifugeSubscriptionState.Subscribed, sub.State);
        }

        /// <summary>
        /// A synchronous Dispose() from a Connected handler while an earlier teardown still
        /// waits for another transport's receive loop (busy in a handler) must not deadlock.
        /// The handler holds that loop until the test saw the outcome.
        /// </summary>
        [Fact]
        public async Task SynchronousDisposeFromConnectedHandlerDuringAnotherTeardown()
        {
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var sub = client.NewSubscription("slow");
            var inHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new ManualResetEventSlim();
            var slow = 0;
            sub.Publication += (_, _) =>
            {
                if (Interlocked.Exchange(ref slow, 1) != 0) return;
                inHandler.TrySetResult();
                release.Wait();
            };
            try
            {
                sub.Subscribe();
                await sub.ReadyAsync(Wait);
                await _server.PublishChannelAsync("slow", Encoding.UTF8.GetBytes("x"));
                await inHandler.Task.WaitAsync(Wait);

                client.Disconnect();
                var disposed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var once = 0;
                client.Connected += (_, _) =>
                {
                    if (Interlocked.Exchange(ref once, 1) != 0) return;
                    client.Dispose();
                    disposed.TrySetResult();
                };
                client.Connect();

                await disposed.Task.WaitAsync(Wait);
                Assert.Equal(CentrifugeClientState.Disconnected, client.State);
            }
            finally
            {
                release.Set();
            }
        }

        /// <summary>
        /// The reconnect a failed connect attempt schedules is bound to that attempt: after
        /// the Error handler restarted the client, it must not replace the new transport. The
        /// attempt is parked at its GetToken and fails with a temporary error released inline:
        /// the Error handler and the reconnect decision have run when the release returns, and
        /// no reconnect is armed.
        /// </summary>
        [Fact]
        public async Task ReconnectOfSupersededConnectAttemptDoesNotReplaceNewTransport()
        {
            var releaseFirst = new TaskCompletionSource<string>();
            var calls = 0;
            var client = NewClient(new CentrifugeClientOptions
            {
                GetToken = () => Interlocked.Increment(ref calls) == 1 ? releaseFirst.Task : Task.FromResult("t"),
            });
            var restarted = 0;
            client.Error += (_, e) =>
            {
                if (e.Type != "connectToken" || Interlocked.Exchange(ref restarted, 1) != 0) return;
                client.Disconnect();
                client.Connect();
            };
            var sub = client.NewSubscription("restart");
            sub.Subscribe();
            client.Connect();
            await UntilAwaited(releaseFirst.Task);

            await CompleteInline(() => releaseFirst.TrySetException(new CentrifugeException(100, "internal", true)));
            Assert.Equal(1, Volatile.Read(ref restarted));
            Assert.Null(Field(client, "_reconnectCts"));

            await sub.ReadyAsync(Wait);
            Assert.Equal(1, _server.Received.Count(c => c.Connect != null));
            Assert.Equal(CentrifugeClientState.Connected, client.State);
        }

        /// <summary>
        /// The no-ping timer of a session that already ended (its callback started before the
        /// reconnect) is bound to that session's transport and doesn't tear down the next one.
        /// </summary>
        [Fact]
        public async Task StaleNoPingDoesNotTearDownNextSession()
        {
            var client = NewClient();
            var connected = 0;
            client.Connected += (_, _) => Interlocked.Increment(ref connected);
            client.Connect();
            await client.ReadyAsync(Wait);
            var first = NoPing.Transport(client)!;

            _server.CloseConnection();
            await Until(() => Volatile.Read(ref connected) == 2);
            client.NoPing(first);

            Assert.Equal(CentrifugeClientState.Connected, client.State);
            Assert.Equal(2, _server.Received.Count(c => c.Connect != null));
        }

        /// <summary>
        /// A teardown completes on the session it detached: when a Connecting handler
        /// disconnects and connects again, the teardown's cleanup and reconnect must not
        /// touch the new connect attempt. NoPing returns once the teardown and its
        /// reconnect decision are done; a send is the barrier for a connect they caused.
        /// </summary>
        [Fact]
        public async Task TeardownDoesNotTouchConnectAttemptStartedByHandler()
        {
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var restarted = 0;
            var connecting = 0;
            client.Connecting += (_, _) =>
            {
                Interlocked.Increment(ref connecting);
                if (Interlocked.Exchange(ref restarted, 1) != 0) return;
                client.Disconnect();
                client.Connect();
            };
            NoPing.Fire(client);

            await client.ReadyAsync(Wait);
            await client.SendAsync(new byte[] { 1 });
            await Until(() => _server.Received.Any(c => c.Send != null));
            Assert.Equal(2, _server.Received.Count(c => c.Connect != null));
            Assert.Equal(2, Volatile.Read(ref connecting));
            Assert.Equal(CentrifugeClientState.Connected, client.State);
        }

        /// <summary>
        /// The end of a session fails the calls of its send stuck mid-write at once, not when the
        /// write gives up: the server stops reading, so the RPC's write can't complete.
        /// </summary>
        [Fact]
        public async Task SessionEndFailsCallsOfSendStuckInWrite()
        {
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _server.BeforeCommand = cmd =>
            {
                if (cmd.Publish == null) return Task.CompletedTask;
                held.TrySetResult();
                return release.Task;
            };
            try
            {
                var client = NewClient(new CentrifugeClientOptions { Timeout = TimeSpan.FromSeconds(30) });
                client.Connect();
                await client.ReadyAsync(Wait);
                _ = client.PublishAsync("channel", new byte[] { 1 });
                await held.Task.WaitAsync(Wait);

                var sendLock = (SemaphoreSlim)Field(NoPing.Transport(client)!, "_sendLock")!;
                var rpc = client.RpcAsync("method", new byte[64 * 1024 * 1024]);
                await Until(() => sendLock.CurrentCount == 0);
                NoPing.Fire(client);

                await Assert.ThrowsAnyAsync<CentrifugeException>(() => rpc.WaitAsync(TimeSpan.FromSeconds(2)));
            }
            finally
            {
                release.TrySetResult();
            }
        }

        /// <summary>
        /// Error 112 (unrecoverable position) of a superseded attempt must not reset the
        /// position the current attempt recovers from.
        /// </summary>
        [Fact]
        public async Task StaleUnrecoverablePositionDoesNotResetNewAttempt()
        {
            var held = HoldFirstSubscribe();
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var getStateCalls = 0;
            var options = WithoutBackoff();
            options.GetState = _ =>
            {
                Interlocked.Increment(ref getStateCalls);
                return Task.FromResult(new CentrifugeStreamPosition(7, "e"));
            };
            var sub = client.NewSubscription("stale-112", options);
            sub.Subscribe();
            var cmd = await held.Task.WaitAsync(Wait);
            sub.Unsubscribe();
            sub.Subscribe();
            await _server.SendReplyAsync(new Reply { Id = cmd.Id, Error = new Error { Code = 112, Message = "unrecoverable position" } });

            await sub.ReadyAsync(Wait);
            Assert.Equal(1, Volatile.Read(ref getStateCalls));
            Assert.Equal(7UL, _server.LastSubscribe()!.Offset);
        }

        /// <summary>
        /// Error 109 (token expired) of a superseded attempt must not make the current
        /// attempt refresh its token.
        /// </summary>
        [Fact]
        public async Task StaleTokenExpiredDoesNotForceRefreshOfNewAttempt()
        {
            var held = HoldFirstSubscribe();
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var getTokenCalls = 0;
            var options = WithoutBackoff();
            options.Token = "t";
            options.GetToken = _ =>
            {
                Interlocked.Increment(ref getTokenCalls);
                return Task.FromResult("fresh");
            };
            var sub = client.NewSubscription("stale-109", options);
            sub.Subscribe();
            var cmd = await held.Task.WaitAsync(Wait);
            sub.Unsubscribe();
            sub.Subscribe();
            await _server.SendReplyAsync(new Reply { Id = cmd.Id, Error = new Error { Code = 109, Message = "token expired" } });

            await sub.ReadyAsync(Wait);
            Assert.Equal(0, Volatile.Read(ref getTokenCalls));
            Assert.Equal("t", _server.LastSubscribe()!.Token);
        }

        [Fact]
        public async Task SupersededGetTokenResultIsNotUsed()
        {
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var firstCall = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseFirst = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var calls = 0;
            var options = WithoutBackoff();
            options.GetToken = _ =>
            {
                if (Interlocked.Increment(ref calls) != 1) return Task.FromResult("fresh");
                firstCall.TrySetResult();
                return releaseFirst.Task;
            };
            var sub = client.NewSubscription("get-token-result", options);
            sub.Subscribe();
            await firstCall.Task.WaitAsync(Wait);

            sub.Unsubscribe();
            sub.Subscribe();
            releaseFirst.TrySetResult("stale");

            await sub.ReadyAsync(Wait);
            Assert.Equal("fresh", _server.LastSubscribe()!.Token);
        }

        /// <summary>
        /// A subscribe timeout or unsubscribe error of a torn-down session must not tear
        /// down the current one.
        /// </summary>
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task StaleSessionFailureDoesNotTearDownNewSession(bool subscribeTimeout)
        {
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);
            var generation = client.ConnectionGeneration;
            NoPing.Fire(client);
            await client.ReadyAsync(Wait);

            var connecting = 0;
            client.Connecting += (_, _) => Interlocked.Increment(ref connecting);
            if (subscribeTimeout) client.HandleSubscribeTimeout(generation);
            else client.HandleUnsubscribeError(generation);

            Assert.Equal(0, Volatile.Read(ref connecting));
            Assert.Equal(CentrifugeClientState.Connected, client.State);
        }

        /// <summary>
        /// A subscription token refresh reply or temporary error of a previous session must
        /// not rearm the refresh timer of the current one. The test drives the refresh and
        /// awaits it: its outcome has been applied, and no refresh timer may be armed.
        /// </summary>
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task StaleSubRefreshOutcomeDoesNotRearmNewSession(bool temporaryError)
        {
            var refresh = new TaskCompletionSource<Command>(TaskCreationOptions.RunContinuationsAsynchronously);
            _server.OnCommand = cmd =>
                cmd.SubRefresh != null && refresh.TrySetResult(cmd) ? FakeCentrifugoServer.NoReply : null;
            var client = NewClient();
            client.Connect();
            await client.ReadyAsync(Wait);

            var subscribed = System.Threading.Channels.Channel.CreateUnbounded<bool>();
            var sub = client.NewSubscription("stale-refresh", new CentrifugeSubscriptionOptions
            {
                Token = "t",
                GetToken = _ => Task.FromResult("t"),
            });
            sub.Subscribed += (_, _) => subscribed.Writer.TryWrite(true);
            sub.Subscribe();
            await subscribed.Reader.ReadAsync().AsTask().WaitAsync(Wait);
            var staleRefresh = sub.RefreshTokenAsync(sub.Epoch, client.ConnectionGeneration);
            var cmd = await refresh.Task.WaitAsync(Wait);

            sub.Unsubscribe();
            sub.Subscribe();
            await subscribed.Reader.ReadAsync().AsTask().WaitAsync(Wait);

            await _server.SendReplyAsync(temporaryError
                ? new Reply { Id = cmd.Id, Error = new Error { Code = 100, Message = "internal", Temporary = true } }
                : new Reply { Id = cmd.Id, SubRefresh = new SubRefreshResult { Expires = true, Ttl = 1 } });
            await staleRefresh.WaitAsync(Wait);

            Assert.Null(Field(sub, "_refreshTimer"));
            Assert.Equal(1, _server.Received.Count(c => c.SubRefresh != null));
            Assert.Equal(CentrifugeSubscriptionState.Subscribed, sub.State);
        }

        /// <summary>
        /// A connection token refresh reply or error of a previous session must neither
        /// rearm the refresh timer of the current one nor disconnect it. Its reply can't
        /// arrive on the new transport (the teardown fails the call), so this drives the
        /// handlers directly; they are synchronous, so no refresh timer may be armed when
        /// they return.
        /// </summary>
        [Theory]
        [InlineData("reply")]
        [InlineData("temporary")]
        [InlineData("permanent")]
        public async Task StaleConnectionRefreshOutcomeDoesNotAffectNewSession(string outcome)
        {
            var getTokenCalls = 0;
            var client = NewClient(new CentrifugeClientOptions
            {
                Token = "t",
                GetToken = () =>
                {
                    Interlocked.Increment(ref getTokenCalls);
                    return Task.FromResult("t");
                },
            });
            var disconnected = 0;
            client.Disconnected += (_, _) => Interlocked.Increment(ref disconnected);
            client.Connect();
            await client.ReadyAsync(Wait);
            const System.Reflection.BindingFlags flags = System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance;
            var epoch = (int)typeof(CentrifugeClient).GetField("_epoch", flags)!.GetValue(client)!;
            NoPing.Fire(client);
            await client.ReadyAsync(Wait);

            if (outcome == "reply")
            {
                typeof(CentrifugeClient).GetMethod("HandleRefreshReply", flags)!
                    .Invoke(client, new object[] { new RefreshResult { Client = "c", Expires = true, Ttl = 1 }, epoch });
            }
            else
            {
                var error = outcome == "temporary"
                    ? new CentrifugeException(100, "internal", true)
                    : new CentrifugeException(103, "permission denied", false);
                typeof(CentrifugeClient).GetMethod("HandleRefreshError", flags)!
                    .Invoke(client, new object[] { error, epoch });
            }

            Assert.Null(Field(client, "_refreshTimer"));
            Assert.Equal(0, Volatile.Read(ref getTokenCalls));
            Assert.Equal(0, Volatile.Read(ref disconnected));
            Assert.Equal(CentrifugeClientState.Connected, client.State);
        }

        /// <summary>A teardown finishing after a newer session subscribed doesn't move the subscription
        /// back to Subscribing; one of its own session does, even after a concurrent Connect.</summary>
        [Fact]
        public async Task DelayedTeardownMovesOnlySubscriptionOfItsSession()
        {
            var client = NewClient(new CentrifugeClientOptions());
            client.Connect();
            await client.ReadyAsync(Wait);
            var endedGeneration = client.ConnectionGeneration;
            NoPing.Fire(client);
            await client.ReadyAsync(Wait);
            var sub = client.NewSubscription("news");
            sub.Subscribe();
            await sub.ReadyAsync(Wait);

            Assert.Null(sub.MoveToSubscribing(CentrifugeSubscribingCodes.TransportClosed, "transport closed", invalidateState: true, endedGeneration));
            Assert.Equal(CentrifugeSubscriptionState.Subscribed, sub.State);

            Assert.NotNull(sub.MoveToSubscribing(CentrifugeSubscribingCodes.TransportClosed, "transport closed", invalidateState: false, client.ConnectionGeneration));
            Assert.Equal(CentrifugeSubscriptionState.Subscribing, sub.State);
        }
    }
}
