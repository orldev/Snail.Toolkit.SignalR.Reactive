namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// Decides whether a transfer may be opened towards a recipient at all.
/// </summary>
/// <remarks>
/// Without it any authenticated client can open transfers towards made-up ids, and each one, buffered for a
/// recipient that will never connect, holds memory until it expires. An application that knows its users answers
/// for them here.
/// </remarks>
public interface ITransferRecipients
{
    /// <summary>
    /// Answers whether the sender may send to the recipient.
    /// </summary>
    /// <param name="senderId">The authenticated sender.</param>
    /// <param name="recipientId">The recipient the sender named.</param>
    /// <param name="cancellationToken">Cancels the check.</param>
    /// <returns><c>true</c> when the transfer may be opened.</returns>
    ValueTask<bool> AcceptsAsync(string senderId, string recipientId, CancellationToken cancellationToken);
}

/// <summary>
/// Accepts every recipient: the default for an application that does not register its own rule.
/// </summary>
public sealed class AnyRecipient : ITransferRecipients
{
    /// <inheritdoc />
    public ValueTask<bool> AcceptsAsync(string senderId, string recipientId, CancellationToken cancellationToken) =>
        ValueTask.FromResult(true);
}
