using Snail.Toolkit.SignalR.Reactive.Transfers;

namespace Snail.Toolkit.SignalR.Reactive;

/// <summary>
/// What the hub calls back on a connected client.
/// </summary>
/// <remarks>
/// Declaring the callbacks as a type is what removes the magic strings from both ends: the hub invokes them
/// through the compiler, and the client registers its handlers under <c>nameof</c> of the same members, so
/// renaming one and forgetting the other stops compiling instead of failing at runtime.
/// </remarks>
public interface IReactiveTransferClient
{
    /// <summary>
    /// Announces an incoming transfer so the recipient can start streaming it.
    /// </summary>
    /// <param name="metadata">The transfer, with its transfer id naming the sender.</param>
    Task PrepareForTransfer(TransferMetadata metadata);

    /// <summary>
    /// Confirms to a sender that its transfer was received.
    /// </summary>
    /// <param name="metadata">The transfer that was received.</param>
    Task AcknowledgeReceipt(TransferMetadata metadata);
}
