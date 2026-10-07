namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// What one transfer asks for, independent of every other transfer.
/// </summary>
/// <param name="Channel">The logical channel the recipient must be listening on.</param>
/// <param name="IsAck">Whether the recipient should acknowledge receipt.</param>
/// <remarks>
/// These used to be mutable fields on the sender, which is registered as a singleton: setting a channel
/// anywhere in the application silently changed every other caller's next transfer.
/// </remarks>
public sealed record TransferOptions(
    string Channel = "default",
    bool IsAck = true)
{
    /// <summary>
    /// The options a transfer gets when the caller asks for nothing in particular.
    /// </summary>
    public static TransferOptions Default { get; } = new();
}
