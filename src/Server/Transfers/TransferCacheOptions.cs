namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// How long, and how much, the hub keeps for parties that are not connected.
/// </summary>
public sealed class TransferCacheOptions
{
    /// <summary>
    /// Gets or sets how many hours a transfer for an offline recipient is kept, counted from when it was stored.
    /// </summary>
    /// <value>
    /// The number of hours. The default is 0, which disables keeping anything.
    /// </value>
    /// <remarks>
    /// Each transfer has its own expiry. The window used to start with the first transfer for a recipient, so one
    /// stored a minute before it closed expired a minute later.
    /// </remarks>
    public int PendingTransferCacheDuration { get; set; }

    /// <summary>
    /// Gets or sets the byte cap on what is kept for a single offline recipient.
    /// </summary>
    /// <value>
    /// The default is 32 MB. Zero removes the cap.
    /// </value>
    /// <remarks>
    /// Kept payload lives in process memory for the whole caching duration, so without a cap one sender
    /// can exhaust the server by streaming to a recipient that never connects.
    /// </remarks>
    public long PendingTransferMaxBytes { get; set; } = 32L * 1024 * 1024;

    /// <summary>
    /// Gets or sets the byte cap on what is kept for every offline recipient together.
    /// </summary>
    /// <value>The default is 512 MB. Zero removes the cap.</value>
    public long PendingTransferTotalBytes { get; set; } = 512L * 1024 * 1024;

    /// <summary>
    /// Gets or sets the share of one recipient's cap a single sender may fill.
    /// </summary>
    /// <value>The default is 0.5.</value>
    /// <remarks>Without it one sender could fill a recipient's whole allowance and lock every other sender out.</remarks>
    public double PendingTransferSenderShare { get; set; } = 0.5;
}
