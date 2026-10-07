namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// Announces transfers and receipts to clients, and hands back to the offline store whatever cannot be delivered now.
/// </summary>
/// <param name="hub">The hub context used to reach clients from outside a hub call.</param>
/// <param name="connections">The connections each user is reachable on.</param>
/// <param name="parcels">The transfers this node holds.</param>
/// <param name="deferred">The transfers and receipts kept for parties that are offline.</param>
/// <param name="clock">The clock announcements are stamped with.</param>
/// <param name="redactor">What the logs may say in place of ids.</param>
/// <param name="logger">The logger for delivery failures.</param>
/// <remarks>
/// Delivery lives outside the hub because a kept transfer is announced long after the connect call that triggered
/// it has returned, on a background worker rather than on the handshake.
/// </remarks>
public sealed class TransferDelivery(
    IHubContext<ReactiveTransferHub, IReactiveTransferClient> hub,
    IConnections connections,
    Parcels parcels,
    IDeferredTransfers deferred,
    TimeProvider clock,
    TransferRedactor redactor,
    ILogger<TransferDelivery> logger)
{
    /// <summary>
    /// Tells a sender the recipient took its transfer, or keeps the receipt until the sender reconnects.
    /// </summary>
    /// <param name="key">The transfer that was taken.</param>
    /// <remarks>The receipt carries the recipient, so a sender that wrote one session to several recipients learns which of them has it.</remarks>
    public async Task AcknowledgeAsync(ParcelKey key)
    {
        var receipt = new TransferReceipt(key.Recipient, key.Session);

        if (connections.Find(key.Sender) is not { } connectionId)
        {
            deferred.StoreReceipt(key.Sender, receipt);
            return;
        }

        await PushReceiptAsync(connectionId, receipt);
    }

    /// <summary>
    /// Tells a recipient a transfer is waiting for it.
    /// </summary>
    /// <param name="parcel">The transfer.</param>
    /// <returns><c>true</c> when the recipient was reached.</returns>
    /// <remarks>The announcement carries the attempt the recipient must echo back when it streams and when it confirms.</remarks>
    public async Task<bool> AnnounceAsync(Parcel parcel)
    {
        if (connections.Find(parcel.Key.Recipient) is not { } connectionId)
            return false;

        parcel.Announce(clock.GetUtcNow());

        try
        {
            await hub.Clients.Client(connectionId).PrepareForTransfer(parcel.Metadata with { TransferId = parcel.Key.Sender, Attempt = parcel.Attempt });
            return true;
        }
        catch (Exception failure)
        {
            parcel.Withdraw();
            logger.LogWarning(failure, "Could not announce a transfer to {Recipient}", redactor.Name(parcel.Key.Recipient));

            return false;
        }
    }

    /// <summary>
    /// Hands a transfer whose recipient went away back to the offline store, so the next connect announces it again.
    /// </summary>
    /// <param name="parcel">The transfer that was announced and not confirmed.</param>
    /// <remarks>
    /// A transfer still being sent stays held and is merely withdrawn: it is stored once the sender completes it.
    /// One the store cannot take is dropped and counted — the store's limits are what bound a recipient that never
    /// comes back.
    /// </remarks>
    public void Return(Parcel parcel)
    {
        if (!parcel.IsSealed)
        {
            parcel.Withdraw();
            return;
        }

        if (!parcel.IsStored && !deferred.TryStore(parcel.ToDeferred(clock.GetUtcNow())))
            parcels.CountRefusal();

        parcels.Remove(parcel);
    }

    /// <summary>
    /// Announces everything waiting for a user that just connected: receipts, transfers held, and transfers kept.
    /// </summary>
    /// <param name="userId">The user that connected.</param>
    public async Task ReplayAsync(string userId)
    {
        if (connections.Find(userId) is not { } connectionId)
            return;

        foreach (var receipt in deferred.DrainReceipts(userId))
            await PushReceiptAsync(connectionId, receipt);

        foreach (var parcel in parcels.For(userId).Where(parcel => !parcel.IsAnnounced))
            await AnnounceAsync(parcel);

        foreach (var stored in deferred.Peek(userId))
        {
            if (parcels.Find(new ParcelKey(stored.SenderId, stored.RecipientId, stored.SessionId)) is not null)
                continue;

            if (parcels.Adopt(stored) is { } adopted)
                await AnnounceAsync(adopted);
        }
    }

    private Task PushReceiptAsync(string connectionId, TransferReceipt receipt) =>
        hub.Clients.Client(connectionId).AcknowledgeReceipt(new TransferMetadata(receipt.RecipientId, receipt.SessionId));
}
