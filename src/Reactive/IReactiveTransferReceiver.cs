namespace Snail.Toolkit.SignalR.Reactive;

/// <summary>
/// Receives chunked binary transfers sent to this client.
/// </summary>
public interface IReactiveTransferReceiver : IAsyncDisposable
{
    /// <summary>
    /// Chooses which logical channel this receiver accepts transfers on.
    /// </summary>
    /// <param name="channel">The channel name.</param>
    /// <remarks>
    /// Startup configuration, not a per-transfer switch: a transfer arriving on any other channel is
    /// refused back to the hub so that the session it is holding is released.
    /// </remarks>
    void SetChannel(string channel);

    /// <summary>
    /// Gets or sets the largest transfer this device accepts; a larger one fails and is refused back to the hub.
    /// </summary>
    /// <value>The default is 64 MB.</value>
    long MaxTransferBytes { get; set; }

    /// <summary>
    /// Gets or sets how many transfers this device streams at once; more wait for a place, and only a long queue is refused.
    /// </summary>
    /// <value>The default is 16.</value>
    int MaxConcurrentTransfers { get; set; }

    /// <summary>
    /// Opens the stream an incoming transfer is written to, given the sender and session ids.
    /// </summary>
    /// <remarks>
    /// Setting this is what turns receiving into real streaming: chunks go straight to the stream and only
    /// one is ever in memory, instead of the whole payload being assembled and then copied again by
    /// <c>ToArray</c>. The receiver disposes the stream it is handed once the transfer ends or fails.
    /// <para>
    /// Leave it unset to keep the buffered behaviour, where <see cref="TransferCompleted"/> hands over the
    /// assembled payload. When it is set, <see cref="TransferStored"/> is raised instead.
    /// </para>
    /// </remarks>
    Func<string, string, Task<Stream>>? OpenDestination { get; set; }

    /// <summary>
    /// Raised once per transfer with the sender's id and the assembled payload.
    /// </summary>
    /// <remarks>
    /// Awaited as part of the chunk pipeline, so a handler that throws fails the transfer instead of
    /// disappearing into an unobserved task. Never raised while <see cref="OpenDestination"/> is set.
    /// </remarks>
    event Func<string, byte[], Task>? TransferCompleted;

    /// <summary>
    /// Raised with the sender's id once a streamed transfer has been written to its destination.
    /// </summary>
    /// <remarks>
    /// The counterpart of <see cref="TransferCompleted"/> for streaming: the payload is already in the
    /// stream, so there is nothing to hand over.
    /// </remarks>
    event Func<string, Task>? TransferStored;

    /// <summary>
    /// Raised for every chunk as it arrives, with the sender's id.
    /// </summary>
    /// <remarks>
    /// Chunks are handed over strictly in order and the next one waits for this handler, which is what lets
    /// a consumer write straight to a stream instead of buffering.
    /// </remarks>
    event Func<string, byte[], Task>? OnChunkReceived;

}
