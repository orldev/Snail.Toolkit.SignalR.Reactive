namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// The wire contract both ends of a transfer agree on.
/// </summary>
public static class TransferProtocol
{
    /// <summary>
    /// The protocol this build speaks.
    /// </summary>
    /// <remarks>
    /// Carried in every <see cref="TransferMetadata"/> and checked when a transfer opens. Without it a
    /// client built against an older shape fails somewhere in the middle of a transfer, with an error that
    /// says nothing about the real cause.
    /// <para>
    /// Version 2: the hub holds every chunk until the recipient confirms, announcements carry an attempt id the
    /// recipient echoes back, completing a transfer that did not get through fails, a refusal is its own call rather than a
    /// confirmation without receipt, and a receipt names the recipient who sent it.
    /// </para>
    /// </remarks>
    public const int Version = 2;
}
