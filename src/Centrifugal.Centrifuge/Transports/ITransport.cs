using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Centrifugal.Centrifuge.Transports
{
    /// <summary>
    /// Transport for communicating with Centrifugo server.
    /// </summary>
    internal interface ITransport : IDisposable
    {
        /// <summary>
        /// Gets the transport type.
        /// </summary>
        CentrifugeTransportType Type { get; }

        /// <summary>
        /// Gets the transport name.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Gets whether this transport uses emulation mode.
        /// Emulation mode is used for unidirectional transports (HTTP Stream at this point)
        /// where sends go through a separate emulation endpoint.
        /// </summary>
        bool UsesEmulation { get; }

        /// <summary>
        /// Event raised when transport is opened and ready to send/receive.
        /// </summary>
        event EventHandler? Opened;

        /// <summary>
        /// Event raised with the messages of one received frame, in order. The whole frame
        /// is passed at once so the client can claim every reply it carries before any
        /// handler runs: a reply already received then can't time out while handlers of
        /// an earlier message of the frame run. The list is only valid during the call.
        /// </summary>
        event EventHandler<IReadOnlyList<byte[]>>? MessageReceived;

        /// <summary>
        /// Event raised when transport is closed.
        /// </summary>
        event EventHandler<TransportClosedEventArgs>? Closed;

        /// <summary>
        /// Event raised when an error occurs.
        /// </summary>
        event EventHandler<Exception>? Error;

        /// <summary>
        /// Opens the transport connection.
        /// </summary>
        /// <param name="cancellationToken">Open deadline set by the client: the transport gives up the
        /// open once it is cancelled.</param>
        /// <param name="initialData">Initial data to send with connection (for emulation transports).</param>
        Task OpenAsync(CancellationToken cancellationToken = default, byte[]? initialData = null);

        /// <summary>
        /// Sends data through the transport.
        /// Data should already be properly formatted (varint-delimited for protocol messages).
        /// A write that fails or is cancelled is followed by Closed: a cancelled one closes the transport,
        /// a failed one fails the later writes and closes it shortly after, unless the close was reported
        /// first (with the server's code when its close frame arrived).
        /// </summary>
        /// <param name="data">Data to send (pre-formatted with varint delimiters).</param>
        /// <param name="cancellationToken">Deadline of the write.</param>
        Task SendAsync(byte[] data, CancellationToken cancellationToken = default);

        /// <summary>
        /// Sends data through the transport using emulation mode.
        /// </summary>
        /// <param name="data">Data to send.</param>
        /// <param name="session">Session ID from connect result.</param>
        /// <param name="node">Node ID from connect result.</param>
        /// <param name="emulationEndpoint">Emulation endpoint URL.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        Task SendEmulationAsync(byte[] data, string session, string node, string emulationEndpoint, CancellationToken cancellationToken = default);

        /// <summary>
        /// Closes the transport.
        /// </summary>
        Task CloseAsync();
    }

    /// <summary>
    /// Event arguments for transport closed event.
    /// </summary>
    internal class TransportClosedEventArgs : EventArgs
    {
        /// <summary>
        /// Gets the close code if available.
        /// </summary>
        public int? Code { get; }

        /// <summary>
        /// Gets the close reason.
        /// </summary>
        public string Reason { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransportClosedEventArgs"/> class.
        /// </summary>
        public TransportClosedEventArgs(int? code = null, string? reason = null)
        {
            Code = code;
            Reason = reason ?? string.Empty;
        }
    }
}
