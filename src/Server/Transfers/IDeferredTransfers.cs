namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// Whole transfers and receipts kept for parties that are not connected.
/// </summary>
/// <remarks>
/// Reading does not remove: a transfer leaves the store only when its recipient confirms it took it, so a recipient
/// that drops before confirming is handed it again. That peek-and-commit shape is also what a durable
/// implementation needs — the shipped one holds everything in the node's memory and loses it on restart; replace it
/// to keep transfers across restarts or nodes.
/// </remarks>
public interface IDeferredTransfers
{
    /// <summary>
    /// Whether keeping transfers for offline parties is configured at all.
    /// </summary>
    bool IsEnabled { get; }

    /// <summary>
    /// Keeps a whole transfer for its recipient, replacing an earlier one with the same sender and session.
    /// </summary>
    /// <param name="session">The sealed transfer.</param>
    /// <returns><c>false</c> when keeping it would cross a limit, or keeping is off.</returns>
    bool TryStore(DeferredSession session);

    /// <summary>
    /// Lists what a recipient can be handed now, oldest first, without removing it.
    /// </summary>
    /// <param name="recipientId">The recipient that connected.</param>
    /// <returns>The transfers that have not expired.</returns>
    IReadOnlyList<DeferredSession> Peek(string recipientId);

    /// <summary>
    /// Removes a transfer its recipient confirmed or refused.
    /// </summary>
    /// <param name="recipientId">The recipient.</param>
    /// <param name="senderId">The sender.</param>
    /// <param name="sessionId">The session.</param>
    void Commit(string recipientId, string senderId, string sessionId);

    /// <summary>
    /// Keeps a receipt for a sender that is not connected.
    /// </summary>
    /// <param name="senderId">The sender the receipt is for.</param>
    /// <param name="receipt">The receipt.</param>
    void StoreReceipt(string senderId, TransferReceipt receipt);

    /// <summary>
    /// Takes the receipts kept for a sender that just connected.
    /// </summary>
    /// <param name="senderId">The sender.</param>
    /// <returns>The receipts, oldest first.</returns>
    IReadOnlyList<TransferReceipt> DrainReceipts(string senderId);

    /// <summary>
    /// Removes whatever has expired.
    /// </summary>
    void Sweep();
}
