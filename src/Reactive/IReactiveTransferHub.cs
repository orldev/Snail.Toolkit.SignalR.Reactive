using Snail.Toolkit.SignalR.Reactive.Transfers;

namespace Snail.Toolkit.SignalR.Reactive;

/// <summary>
/// What a connected client calls on the hub.
/// </summary>
/// <remarks>
/// The counterpart of <see cref="IReactiveTransferClient"/>. The client used to name these calls through
/// <c>nameof</c> of the hub class itself, which tied every client to the assembly the hub lives in and with it to
/// server-side SignalR. Declared here, the hub implements them and the client names them, so a rename on one side
/// still stops compiling on the other while neither package references the other.
/// <para>
/// Internal on purpose: it describes how the client and server packages talk, not something a consumer calls.
/// </para>
/// </remarks>
internal interface IReactiveTransferHub
{
    /// <summary>
    /// Opens a transfer towards the recipient its transfer id names.
    /// </summary>
    Task InitiateTransfer(TransferMetadata metadata);

    /// <summary>
    /// Streams the chunks of one transfer to its recipient.
    /// </summary>
    IAsyncEnumerable<byte[]> StreamBytes(TransferMetadata metadata, CancellationToken cancellationToken);

    /// <summary>
    /// Adds one chunk to an open transfer.
    /// </summary>
    Task SendChunk(byte[] chunk, TransferMetadata metadata);

    /// <summary>
    /// Completes the sending end of a transfer.
    /// </summary>
    Task CompleteTransfer(TransferMetadata metadata);

    /// <summary>
    /// Releases a transfer its recipient took.
    /// </summary>
    Task ReceiverCompleted(TransferMetadata metadata);

    /// <summary>
    /// Records that the recipient will not take a transfer.
    /// </summary>
    Task ReceiverRefused(TransferMetadata metadata);
}
