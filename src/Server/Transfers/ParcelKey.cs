namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// What identifies one transfer on the hub: who sends it, who receives it, and the session the sender named.
/// </summary>
/// <param name="Sender">The authenticated sender.</param>
/// <param name="Recipient">The recipient the sender named.</param>
/// <param name="Session">The session id the sender chose.</param>
/// <remarks>
/// The recipient is part of the key because session ids are the sender's choice: a sender writing one message to
/// several recipients under one session id used to have every copy merged into the first.
/// </remarks>
public readonly record struct ParcelKey(string Sender, string Recipient, string Session);
