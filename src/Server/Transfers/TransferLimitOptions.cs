namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// The limits a hub enforces on what one client can make it hold.
/// </summary>
/// <remarks>
/// Every transfer the hub routes is held in memory until its recipient confirms it, so each of these is a
/// bound on what a single authenticated client can cost the node. None of them can be switched off.
/// </remarks>
public sealed class TransferLimitOptions
{
    /// <summary>
    /// Gets or sets the largest payload one transfer may carry.
    /// </summary>
    /// <value>The default is 64 MB.</value>
    public long MaxTransferBytes { get; set; } = 64L * 1024 * 1024;

    /// <summary>
    /// Gets or sets how many transfers one sender may have open at once.
    /// </summary>
    /// <value>The default is 64.</value>
    public int MaxOpenTransfersPerSender { get; set; } = 64;

    /// <summary>
    /// Gets or sets how many bytes of open and undelivered transfers this node holds in all.
    /// </summary>
    /// <value>The default is 1 GB.</value>
    public long MaxHeldBytes { get; set; } = 1024L * 1024 * 1024;

    /// <summary>
    /// Gets or sets how long a transfer that is still being sent may go without a chunk before it is dropped.
    /// </summary>
    /// <value>The default is two minutes.</value>
    /// <remarks>A sender that opens a transfer and never completes it would otherwise hold its chunks forever.</remarks>
    public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Gets or sets how long an announced transfer may wait for its recipient's confirmation.
    /// </summary>
    /// <value>The default is five minutes.</value>
    /// <remarks>
    /// After this the transfer goes back to the offline buffer, when buffering is on, and is announced again on the
    /// recipient's next connect: a recipient that crashed mid-way must not lose it.
    /// </remarks>
    public TimeSpan DeliveryTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets the longest transfer or session id a client may send.
    /// </summary>
    /// <value>The default is 128 characters.</value>
    public int MaxIdLength { get; set; } = 128;
}
