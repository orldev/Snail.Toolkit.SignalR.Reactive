namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// Represents metadata associated with a transfer operation.
/// </summary>
/// <param name="TransferId">The other party: the recipient when the sender opens a transfer, the sender when the hub announces it, the recipient again on a receipt.</param>
/// <param name="SessionId">The session identifier the sender chose.</param>
/// <param name="Channel">The channel through which the transfer occurs. Defaults to "default".</param>
/// <param name="IsAck">Whether the sender wants a receipt once the recipient has the transfer. Defaults to true.</param>
/// <param name="Version">The protocol the sender speaks.</param>
/// <param name="Attempt">The hub's id for this opening of the session, which the recipient echoes back.</param>
public sealed record TransferMetadata(
    string TransferId,
    string SessionId,
    string Channel = "default",
    bool IsAck = true,
    int Version = TransferProtocol.Version,
    string? Attempt = null)
{
    /// <summary>
    /// Describes the transfer without saying who takes part in it.
    /// </summary>
    /// <returns>The channel, the receipt flag and the protocol version.</returns>
    /// <remarks>
    /// SignalR's own trace logging prints every hub argument through this method. The generated record version
    /// wrote out the transfer and session ids, so a relay logging at trace level recorded who wrote to whom.
    /// </remarks>
    public override string ToString() => $"TransferMetadata {{ Channel = {Channel}, IsAck = {IsAck}, Version = {Version} }}";
}
