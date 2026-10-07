namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// A recipient's confirmation that it took a transfer.
/// </summary>
/// <param name="RecipientId">The recipient who confirmed.</param>
/// <param name="SessionId">The session it confirmed.</param>
/// <remarks>
/// It names the recipient because a sender may use one session id towards several recipients; a receipt with the
/// session alone could not say which of them has it.
/// </remarks>
public sealed record TransferReceipt(string RecipientId, string SessionId);
