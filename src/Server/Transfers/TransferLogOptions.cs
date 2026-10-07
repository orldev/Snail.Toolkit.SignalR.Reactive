namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// How much the hub's logs may say about who is talking to whom.
/// </summary>
public sealed class TransferLogOptions
{
    /// <summary>
    /// Gets or sets a value indicating whether logs carry real user and session ids.
    /// </summary>
    /// <value>The default is <c>false</c>: logs carry per-process pseudonyms instead.</value>
    /// <remarks>
    /// A relay's log of who sent what to whom, and when, is the social graph of its users. The pseudonyms still let
    /// one log line be matched to another, but they change on every restart and cannot be reversed.
    /// </remarks>
    public bool RevealIdentities { get; set; }
}
