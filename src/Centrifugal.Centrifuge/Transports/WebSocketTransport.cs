using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;

namespace Centrifugal.Centrifuge.Transports
{
    /// <summary>
    /// WebSocket transport for Centrifugo.
    /// </summary>
    internal class WebSocketTransport : ITransport
    {
        private readonly Uri _uri;
        private readonly string _subProtocol;
        private ClientWebSocket? _webSocket;
        private CancellationTokenSource? _receiveCts;
        private Task? _receiveTask;
        private readonly SemaphoreSlim _sendLock = new SemaphoreSlim(1, 1);
        private int _disposed;
        /// <summary>Stopwatch timestamp of a failed write, 0 while none: later writes fail at once, so nothing
        /// is sent past the lost data.</summary>
        private long _writeFailedAt;
        /// <summary>The receive loop's current read (a sequence number) and whether it is waiting on the
        /// network rather than dispatching: the grace after a failed write counts only network time.</summary>
        private long _readSeq;
        private int _reading;

        /// <summary>How long a failed write leaves the receive loop, waiting on the network, to report the
        /// server's close before the socket is aborted.</summary>
        private static readonly TimeSpan FailedWriteCloseGrace = TimeSpan.FromSeconds(1);

        /// <inheritdoc/>
        public CentrifugeTransportType Type => CentrifugeTransportType.WebSocket;

        /// <inheritdoc/>
        public string Name => "websocket";

        /// <inheritdoc/>
        public bool UsesEmulation => false;

        /// <inheritdoc/>
        public event EventHandler? Opened;

        /// <inheritdoc/>
        public event EventHandler<IReadOnlyList<byte[]>>? MessageReceived;

        /// <inheritdoc/>
        public event EventHandler<TransportClosedEventArgs>? Closed;

        /// <inheritdoc/>
        public event EventHandler<Exception>? Error;

        /// <summary>
        /// Initializes a new instance of the <see cref="WebSocketTransport"/> class.
        /// </summary>
        /// <param name="endpoint">WebSocket endpoint URL.</param>
        public WebSocketTransport(string endpoint)
        {
            if (string.IsNullOrWhiteSpace(endpoint))
            {
                throw new ArgumentException("Endpoint cannot be null or empty", nameof(endpoint));
            }

            _uri = new Uri(endpoint);
            _subProtocol = "centrifuge-protobuf";
        }

        /// <inheritdoc/>
        /// <remarks>A disposed transport doesn't open: the socket and the receive token source are
        /// published (full fence) before the disposed checks, so a concurrent Dispose either disposes
        /// them or is seen here — before connecting or before the receive loop starts.</remarks>
        public async Task OpenAsync(CancellationToken cancellationToken = default, byte[]? initialData = null)
        {
            if (_webSocket != null)
            {
                throw new InvalidOperationException("Transport is already open");
            }

            // WebSocket doesn't use initialData parameter
            var webSocket = new ClientWebSocket();
            webSocket.Options.AddSubProtocol(_subProtocol);
            var receiveCts = new CancellationTokenSource();
            Interlocked.Exchange(ref _receiveCts, receiveCts);
            Interlocked.Exchange(ref _webSocket, webSocket);
            if (Volatile.Read(ref _disposed) != 0)
            {
                webSocket.Dispose();
                receiveCts.Dispose();
                throw new ObjectDisposedException(nameof(WebSocketTransport));
            }

            try
            {
                await webSocket.ConnectAsync(_uri, cancellationToken).ConfigureAwait(false);
                if (Volatile.Read(ref _disposed) != 0) throw new ObjectDisposedException(nameof(WebSocketTransport));

                _receiveTask = ReceiveLoopAsync(receiveCts.Token);

                Opened?.Invoke(this, EventArgs.Empty);
            }
            catch (Exception ex)
            {
                _webSocket?.Dispose();
                _webSocket = null;
                throw new CentrifugeException(CentrifugeErrorCodes.TransportClosed, "Failed to connect WebSocket", true, ex);
            }
        }

        /// <inheritdoc/>
        /// <remarks>A write that outlives <paramref name="cancellationToken"/> aborts the socket. A failed one
        /// fails the later writes at once and leaves the receive loop <see cref="FailedWriteCloseGrace"/> of
        /// network wait in total to report the server's close (a close frame may already be buffered): the
        /// read in progress is aborted after it, later reads spend what is left; time spent dispatching
        /// doesn't count. Either way the receive loop reports the close, once.</remarks>
        public async Task SendAsync(byte[] data, CancellationToken cancellationToken = default)
        {
            bool locked = false;
            var webSocket = _webSocket;
            try
            {
                await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
                locked = true;
                if (webSocket == null || webSocket.State != WebSocketState.Open || Interlocked.Read(ref _writeFailedAt) != 0)
                {
                    throw new CentrifugeException(CentrifugeErrorCodes.TransportClosed, "WebSocket is not open");
                }

                await webSocket.SendAsync(
                    new ArraySegment<byte>(data),
                    WebSocketMessageType.Binary,
                    true,
                    cancellationToken
                ).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                webSocket?.Abort();
                throw;
            }
            catch (Exception) when (webSocket != null)
            {
                if (Interlocked.CompareExchange(ref _writeFailedAt, Stopwatch.GetTimestamp(), 0) == 0
                    && Volatile.Read(ref _reading) != 0)
                    _ = AbortReadAfterGraceAsync(webSocket, Interlocked.Read(ref _readSeq));
                throw;
            }
            finally
            {
                if (locked) _sendLock.Release();
            }
        }

        /// <summary>Aborts the socket if the read <paramref name="readSeq"/>, in progress when a write failed,
        /// is still waiting on the network after the grace.</summary>
        private async Task AbortReadAfterGraceAsync(ClientWebSocket webSocket, long readSeq)
        {
            await Task.Delay(FailedWriteCloseGrace).ConfigureAwait(false);
            if (Volatile.Read(ref _reading) != 0 && Interlocked.Read(ref _readSeq) == readSeq) webSocket.Abort();
        }

        /// <inheritdoc/>
        public Task SendEmulationAsync(byte[] data, string session, string node, string emulationEndpoint, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("WebSocket transport does not use emulation mode");
        }

        /// <inheritdoc/>
        /// <remarks>The close frame goes first, as a send (after a pending write; bounded): cancelling a
        /// pending receive aborts the socket, after which it couldn't. CloseOutputAsync only sends it — no
        /// acknowledgement is awaited, and a receive may run meanwhile. Then the receive loop is cancelled
        /// and awaited. Close errors are ignored: the connection is torn down anyway.</remarks>
        public async Task CloseAsync()
        {
            var webSocket = _webSocket;
            if (webSocket == null) return;
            var cts = _receiveCts;
            var receiveTask = _receiveTask;

            using (var closeCts = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
            {
                bool locked = false;
                try
                {
                    await _sendLock.WaitAsync(closeCts.Token).ConfigureAwait(false);
                    locked = true;
                    if (webSocket.State == WebSocketState.Open || webSocket.State == WebSocketState.CloseReceived)
                    {
                        await webSocket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "Client disconnect", closeCts.Token)
                            .ConfigureAwait(false);
                    }
                }
                catch
                {
                }
                finally
                {
                    if (locked) _sendLock.Release();
                }
            }

            try { cts?.Cancel(); } catch (ObjectDisposedException) { }
            if (receiveTask != null)
            {
                try
                {
                    await receiveTask.ConfigureAwait(false);
                }
                catch
                {
                }
            }
        }

        /// <summary>
        /// Receive loop that processes incoming messages. Each WebSocket message is raised in order
        /// as one frame, also the messages before a malformed one; the parse error closes the transport.
        /// Only the loop's own cancellation is a normal stop: any other cancellation is a failed transport.
        /// The message buffer returns to ReceiveBufferSize after each message: a large one isn't kept
        /// allocated, at the cost of growing again for the next. Reads after a failed write share
        /// <see cref="FailedWriteCloseGrace"/> of network wait (see SendAsync).
        /// </summary>
        private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(16 * 1024);
            var ms = new MemoryStream();
            var frame = new List<byte[]>();
            // Capture the socket as a local so a concurrent Dispose nulling _webSocket
            // (future-proofing) can't cause NREs mid-loop.
            var webSocket = _webSocket;
            var graceLeft = FailedWriteCloseGrace;

            try
            {
                while (!cancellationToken.IsCancellationRequested && webSocket != null)
                {
                    WebSocketReceiveResult result;
                    ms.SetLength(0);
                    if (ms.Capacity > VarintCodec.ReceiveBufferSize) ms.Capacity = VarintCodec.ReceiveBufferSize;

                    // Read the complete WebSocket message
                    do
                    {
                        Interlocked.Increment(ref _readSeq);
                        Interlocked.Exchange(ref _reading, 1);
                        var readStart = Stopwatch.GetTimestamp();
                        try
                        {
                            if (Interlocked.Read(ref _writeFailedAt) == 0)
                            {
                                result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), cancellationToken).ConfigureAwait(false);
                            }
                            else
                            {
                                using var grace = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                                grace.CancelAfter(graceLeft > TimeSpan.Zero ? graceLeft : TimeSpan.Zero);
                                result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), grace.Token).ConfigureAwait(false);
                            }
                        }
                        finally
                        {
                            Volatile.Write(ref _reading, 0);
                        }
                        var failedAt = Interlocked.Read(ref _writeFailedAt);
                        if (failedAt != 0)
                        {
                            graceLeft -= TimeSpan.FromSeconds(
                                (Stopwatch.GetTimestamp() - Math.Max(readStart, failedAt)) / (double)Stopwatch.Frequency);
                        }

                        if (result.MessageType == WebSocketMessageType.Close)
                        {
                            Closed?.Invoke(this, new TransportClosedEventArgs(
                                (int?)result.CloseStatus,
                                result.CloseStatusDescription
                            ));
                            return;
                        }

                        ms.Write(buffer, 0, result.Count);
                    } while (!result.EndOfMessage);

                    try
                    {
                        var length = (int)ms.Length;
                        if (VarintCodec.ReadCompleteMessages(ms.GetBuffer(), length, frame) != length)
                            throw new IOException("Truncated message in WebSocket frame");
                    }
                    finally
                    {
                        if (frame.Count > 0) MessageReceived?.Invoke(this, frame);
                        frame.Clear();
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                Error?.Invoke(this, ex);
                Closed?.Invoke(this, new TransportClosedEventArgs());
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
                ms.Dispose();
            }
        }

        /// <inheritdoc/>
        /// <remarks>A concurrent OpenAsync that saw the disposal may have released the token source already.
        /// The send lock is left undisposed: a sender waiting on it gets it and fails on the closed socket.</remarks>
        public void Dispose()
        {
            if (System.Threading.Interlocked.CompareExchange(ref _disposed, 1, 0) != 0) return;

            try { _receiveCts?.Cancel(); } catch (ObjectDisposedException) { }
            _receiveCts?.Dispose();
            _webSocket?.Dispose();
        }
    }
}
