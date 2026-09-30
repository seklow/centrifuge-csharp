using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Centrifugal.Centrifuge
{
    /// <summary>
    /// The waiters of a ready state (ReadyAsync of the client or a subscription). The owner checks its
    /// state and adds a waiter in one critical section of its state lock, and resolves or rejects them
    /// under that lock: no waiter is added between a state check and a resolve.
    /// </summary>
    internal sealed class ReadyPromises
    {
        private readonly ConcurrentDictionary<int, TaskCompletionSource<bool>> _promises = new();
        private int _nextId;

        /// <summary>Adds a waiter, ended by the (validated) <paramref name="timeout"/> or the cancellation
        /// too. Call under the owner's state lock, after its state check.</summary>
        public Task Add(TimeSpan? timeout, CancellationToken cancellationToken)
        {
            var promise = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            int id = Interlocked.Increment(ref _nextId);
            _promises[id] = promise;

            CancellationTokenSource? timeoutCts = null;
            CancellationTokenRegistration timeoutRegistration = default;
            if (timeout.HasValue)
            {
                timeoutCts = new CancellationTokenSource(timeout.Value);
                timeoutRegistration = timeoutCts.Token.Register(() =>
                {
                    if (_promises.TryRemove(id, out var p)) p.TrySetException(new CentrifugeTimeoutException("timeout"));
                });
            }

            var cancellationRegistration = cancellationToken.Register(() =>
            {
                if (_promises.TryRemove(id, out var p)) p.TrySetCanceled(cancellationToken);
            });

            promise.Task.ContinueWith(_ =>
            {
                timeoutRegistration.Dispose();
                cancellationRegistration.Dispose();
                timeoutCts?.Dispose();
            }, TaskContinuationOptions.ExecuteSynchronously);

            return promise.Task;
        }

        /// <summary>Completes every waiter. Call under the owner's state lock.</summary>
        public void ResolveAll()
        {
            foreach (var id in _promises.Keys)
            {
                if (_promises.TryRemove(id, out var promise)) promise.TrySetResult(true);
            }
        }

        /// <summary>Fails every waiter with <paramref name="error"/>. Call under the owner's state lock.</summary>
        public void RejectAll(Exception error)
        {
            foreach (var id in _promises.Keys)
            {
                if (_promises.TryRemove(id, out var promise)) promise.TrySetException(error);
            }
        }
    }

    /// <summary>
    /// Raising the events of a client or subscription: a handler's exception is reported through
    /// its Error event and doesn't alter the SDK flow.
    /// </summary>
    internal static class EventDispatch
    {
        /// <summary>Invocation lists of event delegates (immutable), built once per delegate instance
        /// rather than on every raise.</summary>
        private static readonly ConditionalWeakTable<Delegate, Delegate[]> InvocationLists = new ConditionalWeakTable<Delegate, Delegate[]>();

        /// <summary>
        /// Raises an event to each subscriber in turn. A subscriber's exception is reported as an error
        /// of type <paramref name="type"/> — or logged without an Error subscriber: the other
        /// subscribers, the transition, its cleanup and the events that follow still run.
        /// </summary>
        public static void Raise<TArgs>(object sender, EventHandler<TArgs>? handler, TArgs args, string type,
            EventHandler<CentrifugeErrorEventArgs>? error, ILogger? logger)
        {
            if (handler == null) return;
#if NET9_0_OR_GREATER
            if (handler.HasSingleTarget)
            {
                Invoke(handler);
                return;
            }
#endif
            foreach (var subscriber in InvocationListOf(handler)) Invoke((EventHandler<TArgs>)subscriber);

            void Invoke(EventHandler<TArgs> subscriber)
            {
                try
                {
                    subscriber(sender, args);
                }
                catch (Exception ex)
                {
                    if (error == null) Log(logger, ex, type + " handler threw");
                    else RaiseError(sender, error, type, ex, logger);
                }
            }
        }

        /// <summary>
        /// Raises an Error event to each subscriber in turn, with the code and temporariness of a
        /// <see cref="CentrifugeException"/>. It is the last place an exception can be reported: one a
        /// subscriber throws is logged, not let into the SDK flow that reports the error.
        /// </summary>
        public static void RaiseError(object sender, EventHandler<CentrifugeErrorEventArgs>? handler,
            string type, Exception exception, ILogger? logger)
        {
            if (handler == null) return;
            var args = exception is CentrifugeException centrifugeException
                ? new CentrifugeErrorEventArgs(type, centrifugeException.Code, exception.Message, centrifugeException.Temporary, exception)
                : new CentrifugeErrorEventArgs(type, 0, exception.Message, false, exception);
            Raise(sender, handler, args, "Error event", null, logger);
        }

        private static Delegate[] InvocationListOf(Delegate handler) =>
            InvocationLists.GetValue(handler, d => d.GetInvocationList());

        private static void Log(ILogger? logger, Exception exception, string message) =>
            logger?.LogError(exception, message);
    }

    /// <summary>
    /// The application's logger behind a barrier: an exception it throws doesn't enter the SDK flow —
    /// a lifecycle transition, an async void transport handler, an error report.
    /// </summary>
    internal sealed class GuardedLogger : ILogger
    {
        private readonly ILogger _inner;

        private GuardedLogger(ILogger inner) => _inner = inner;

        public static ILogger? Wrap(ILogger? logger) => logger == null ? null : new GuardedLogger(logger);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            try
            {
                _inner.Log(logLevel, eventId, state, exception, formatter);
            }
            catch
            {
            }
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            try
            {
                return _inner.IsEnabled(logLevel);
            }
            catch
            {
                return false;
            }
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            try
            {
                return _inner.BeginScope(state);
            }
            catch
            {
                return NoScope.Instance;
            }
        }

        private sealed class NoScope : IDisposable
        {
            public static readonly NoScope Instance = new NoScope();

            public void Dispose()
            {
            }
        }
    }

    /// <summary>
    /// Utility methods for the Centrifuge client.
    /// </summary>
    internal static class Utilities
    {
        private static readonly Random Random = new Random();

        /// <summary>Longest interval the timers of every supported runtime accept.</summary>
        public static readonly TimeSpan MaxTimerInterval = TimeSpan.FromMilliseconds(int.MaxValue);

        /// <summary>Throws unless a wait timeout is none, infinite or within 0..<see cref="MaxTimerInterval"/>.</summary>
        public static void ValidateWaitTimeout(TimeSpan? timeout, string paramName)
        {
            if (timeout is { } t && t != System.Threading.Timeout.InfiniteTimeSpan && (t < TimeSpan.Zero || t > MaxTimerInterval))
                throw new ArgumentOutOfRangeException(paramName, t, "The timeout must be infinite or between zero and MaxTimerInterval.");
        }

        /// <summary>
        /// Calculates backoff delay with full jitter.
        /// Full jitter technique from: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
        /// </summary>
        /// <param name="attempt">The attempt number (0-based).</param>
        /// <param name="minDelay">Minimum delay.</param>
        /// <param name="maxDelay">Maximum delay.</param>
        /// <returns>Delay in milliseconds, at least 1: a retry loop waiting it always yields.</returns>
        public static int CalculateBackoff(int attempt, TimeSpan minDelay, TimeSpan maxDelay)
        {
            if (attempt < 0) attempt = 0;
            if (attempt > 31) attempt = 31; // Prevent overflow

            double minDelayMs = minDelay.TotalMilliseconds;
            double maxDelayMs = maxDelay.TotalMilliseconds;

            // Calculate exponential backoff: minDelay * 2^attempt
            double exponential = minDelayMs * Math.Pow(2, attempt);

            // Calculate the random interval: [0, min(maxDelay, minDelay * 2^attempt)]
            double intervalMax = Math.Min(maxDelayMs, exponential);

            double interval;
            lock (Random)
            {
                interval = Random.NextDouble() * intervalMax;
            }

            // Return min + interval, capped at maxDelay
            return Math.Max(1, (int)Math.Min(maxDelayMs, minDelayMs + interval));
        }

        /// <summary>
        /// Converts TTL in seconds to milliseconds, accounting for clock skew.
        /// </summary>
        /// <param name="ttl">TTL in seconds.</param>
        /// <returns>
        /// TTL in milliseconds, reduced by a small amount to account for network delays,
        /// clamped to <see cref="int.MaxValue"/> (~24.8 days).
        /// </returns>
        public static int TtlToMilliseconds(uint ttl)
        {
            if (ttl == 0) return 0;

            // Reduce by 5% to account for clock skew and network delays.
            // Widen to double before multiplying: uint arithmetic wraps for TTLs above
            // ~49 days (e.g. a 1 year token TTL), which would schedule the refresh far
            // too early - a 49.7 day TTL wrapped to a ~670ms delay.
            double ms = (double)ttl * 1000 * 0.95;

            // Clamp so the result stays a valid Timer due time. Refreshing a token
            // earlier than strictly necessary is harmless.
            if (ms > int.MaxValue) return int.MaxValue;

            return (int)Math.Max(1, ms);
        }

        /// <summary>
        /// No-ping deadline: the server ping interval (seconds) plus <paramref name="maxDelay"/>, in
        /// milliseconds, clamped to <see cref="int.MaxValue"/> so it stays a valid Timer due time.
        /// </summary>
        public static int PingDeadlineMilliseconds(uint pingInterval, TimeSpan maxDelay)
        {
            double ms = (double)pingInterval * 1000 + maxDelay.TotalMilliseconds;
            return ms > int.MaxValue ? int.MaxValue : (int)ms;
        }

        /// <summary>
        /// Waits for <paramref name="task"/> or the cancellation of <paramref name="cancellationToken"/>,
        /// whichever comes first; true when the task completed. The task's fault is left to the caller.
        /// The registration on the token ends with the wait, so a long-lived token doesn't accumulate one
        /// per call; a token that can't be cancelled needs no race.
        /// </summary>
        public static async Task<bool> CompletesBeforeCancellationAsync(Task task, CancellationToken cancellationToken)
        {
            if (!cancellationToken.CanBeCanceled)
            {
                try { await task.ConfigureAwait(false); }
                catch { }
                return true;
            }

            var cancelled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (cancellationToken.Register(state => ((TaskCompletionSource<bool>)state!).TrySetResult(true), cancelled))
            {
                return await Task.WhenAny(task, cancelled.Task).ConfigureAwait(false) == task;
            }
        }
    }

    /// <summary>
    /// Varint encoder/decoder for Protobuf messages.
    /// </summary>
    internal static class VarintCodec
    {
        /// <summary>
        /// Receive buffer size a transport keeps between messages: a larger message grows the buffer
        /// only until it is consumed, so a single one doesn't pin its capacity for the session.
        /// </summary>
        public const int ReceiveBufferSize = 16 * 1024;

        /// <summary>Array.MaxLength for bytes (not exposed by netstandard2.1).</summary>
        private const int MaxBufferLength = 0x7FFFFFC7;

        /// <summary>
        /// Grows a receive buffer full of the incomplete message it starts with, keeping its
        /// <paramref name="count"/> bytes: doubled, up to the message's size once its length prefix is
        /// read. The buffer grows with the data that arrives, never to a size a (possibly corrupt)
        /// prefix only claims.
        /// </summary>
        public static byte[] Grow(byte[] buffer, int count)
        {
            long size = (long)buffer.Length * 2;
            if (TryReadLength(buffer, 0, count, out int length, out int dataStart))
                size = Math.Min(size, (long)dataStart + length);
            if (size > MaxBufferLength) throw new IOException("Message exceeds the maximum buffer size");
            var grown = new byte[size];
            Buffer.BlockCopy(buffer, 0, grown, 0, count);
            return grown;
        }

        /// <summary>
        /// Moves the <paramref name="remaining"/> unread bytes after <paramref name="consumed"/> to the
        /// start of a buffer, returning to <see cref="ReceiveBufferSize"/> once they fit in it.
        /// </summary>
        public static byte[] Compact(byte[] buffer, int consumed, int remaining)
        {
            var target = buffer.Length > ReceiveBufferSize && remaining <= ReceiveBufferSize
                ? new byte[ReceiveBufferSize]
                : buffer;
            Buffer.BlockCopy(buffer, consumed, target, 0, remaining);
            return target;
        }

        /// <summary>
        /// Reads the complete varint-delimited messages at the start of a buffer, leaving an
        /// incomplete trailing message unread: a stream transport keeps it for the next chunk, a
        /// message transport treats it as a malformed message.
        /// </summary>
        /// <param name="buffer">The buffer to read from.</param>
        /// <param name="count">The number of bytes in the buffer.</param>
        /// <param name="messages">The list the messages are added to.</param>
        /// <returns>The number of bytes consumed.</returns>
        public static int ReadCompleteMessages(byte[] buffer, int count, List<byte[]> messages)
        {
            int consumed = 0;
            while (consumed < count)
            {
                if (!TryReadLength(buffer, consumed, count, out int length, out int position) || count - position < length)
                    return consumed;
                var message = length == 0 ? Array.Empty<byte>() : new byte[length];
                Buffer.BlockCopy(buffer, position, message, 0, length);
                messages.Add(message);
                consumed = position + length;
            }
            return consumed;
        }

        /// <summary>
        /// Reads the varint length prefix of the message at <paramref name="position"/>: false while
        /// the prefix is incomplete, otherwise the length and where the message's data starts. A
        /// malformed prefix, or one claiming a message larger than the largest byte array, is a broken
        /// stream — rejected before anything is allocated for it.
        /// </summary>
        private static bool TryReadLength(byte[] buffer, int position, int count, out int length, out int dataStart)
        {
            int start = position;
            ulong value = 0;
            int shift = 0;
            while (true)
            {
                if (position == count)
                {
                    length = 0;
                    dataStart = 0;
                    return false;
                }
                byte b = buffer[position++];
                value |= (ulong)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
                if (shift >= 32) throw new IOException("Varint too long");
            }

            if (value > (ulong)(MaxBufferLength - (position - start))) throw new IOException("Message length out of range");
            length = (int)value;
            dataStart = position;
            return true;
        }

        /// <summary>
        /// Writes a varint-delimited message to a stream.
        /// </summary>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="data">The message data.</param>
        public static void WriteDelimitedMessage(Stream stream, byte[] data)
        {
            // Write the varint length prefix
            WriteVarint(stream, data.Length);

            // Write the message data
            stream.Write(data, 0, data.Length);
        }

        /// <summary>
        /// Writes a varint to a stream.
        /// </summary>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="value">The value to encode.</param>
        private static void WriteVarint(Stream stream, int value)
        {
            uint uvalue = (uint)value;
            byte[] buffer = ArrayPool<byte>.Shared.Rent(5);
            try
            {
                int index = 0;
                while (uvalue > 0x7F)
                {
                    buffer[index++] = (byte)((uvalue & 0x7F) | 0x80);
                    uvalue >>= 7;
                }
                buffer[index++] = (byte)uvalue;
                stream.Write(buffer, 0, index);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }
}
