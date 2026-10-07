namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// One complete transfer kept for a recipient that is not connected.
/// </summary>
/// <param name="SenderId">The sender the chunks came from.</param>
/// <param name="RecipientId">The recipient it waits for.</param>
/// <param name="SessionId">The session the chunks belong to.</param>
/// <param name="Channel">The channel the sender sent it on, which the recipient must be listening on.</param>
/// <param name="IsAck">Whether the sender wants a receipt.</param>
/// <param name="Chunks">The payload chunks in the order they were sent.</param>
/// <param name="StoredAt">When it was stored, which its expiry counts from.</param>
/// <remarks>
/// Only whole transfers are stored: a transfer is kept once its sender completed it, so a recipient is never handed
/// half of one. The channel and the receipt flag are kept with it because a replay that dropped them used to
/// deliver every buffered transfer on the default channel with a receipt the sender never asked for.
/// </remarks>
public sealed record DeferredSession(
    string SenderId,
    string RecipientId,
    string SessionId,
    string Channel,
    bool IsAck,
    IReadOnlyList<byte[]> Chunks,
    DateTimeOffset StoredAt)
{
    /// <summary>
    /// Gets how many payload bytes it holds.
    /// </summary>
    public long Bytes => Chunks.Sum(chunk => (long)chunk.Length);
}
