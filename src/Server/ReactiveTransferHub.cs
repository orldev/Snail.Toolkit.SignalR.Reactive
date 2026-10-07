using System.Runtime.CompilerServices;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Options;
using Snail.Toolkit.SignalR.Reactive.Transfers;

namespace Snail.Toolkit.SignalR.Reactive;

/// <summary>
/// Routes chunked binary transfers between authenticated clients.
/// </summary>
/// <param name="parcels">The transfers this node holds.</param>
/// <param name="connections">The connections each user is reachable on.</param>
/// <param name="deferred">The transfers kept for parties that are offline.</param>
/// <param name="delivery">The delivery that announces transfers and receipts.</param>
/// <param name="backlog">The queue of users whose waiting transfers still have to be announced.</param>
/// <param name="recipients">The rule for who may be sent to.</param>
/// <param name="limits">The limits on what one client may make the hub hold.</param>
/// <param name="clock">The clock completions are stamped with.</param>
/// <param name="redactor">What the logs may say in place of ids.</param>
/// <param name="logger">The logger for routing decisions.</param>
/// <remarks>
/// The hub only routes: holding, keeping and announcing each live in their own type, and the hub is left holding
/// no state of its own.
/// <para>
/// Ownership comes from <c>Context.UserIdentifier</c> alone. Transfer and session identifiers arrive inside
/// client-supplied metadata and are untrusted: a transfer is found by its sender, its recipient and its session
/// together, and the caller is always one of the first two, so nobody reaches a transfer that is not theirs.
/// </para>
/// <para>
/// Every way a transfer can fail to get through — refused, too large, nowhere to keep it — makes completing it
/// throw. A sender that is told its transfer was accepted can rely on it.
/// </para>
/// </remarks>
[Authorize]
public class ReactiveTransferHub(
    Parcels parcels,
    IConnections connections,
    IDeferredTransfers deferred,
    TransferDelivery delivery,
    TransferBacklog backlog,
    ITransferRecipients recipients,
    IOptions<TransferLimitOptions> limits,
    TimeProvider clock,
    TransferRedactor redactor,
    ILogger<ReactiveTransferHub> logger) : Hub<IReactiveTransferClient>, IReactiveTransferHub
{
    /// <summary>
    /// Opens a transfer towards a recipient, announcing it at once when the recipient is connected.
    /// </summary>
    /// <param name="metadata">The transfer to open; its transfer id names the recipient.</param>
    /// <exception cref="HubException">
    /// The client speaks another protocol, sent ids it may not send, named a recipient it may not send to, has too
    /// much open already, or names a recipient who is offline while nothing is kept for offline recipients.
    /// </exception>
    /// <remarks>
    /// Opening a session that is already open replaces it: the sender is retrying, and the earlier attempt is
    /// refused rather than having the retry's chunks appended to it.
    /// </remarks>
    public async Task InitiateTransfer(TransferMetadata metadata)
    {
        if (metadata.Version != TransferProtocol.Version)
            throw new HubException($"Unsupported transfer protocol {metadata.Version}; this hub speaks {TransferProtocol.Version}");

        var sender = Sender();
        if (!IsValid(metadata))
            throw new HubException("Invalid transfer");

        if (!await recipients.AcceptsAsync(sender, metadata.TransferId, Context.ConnectionAborted))
            throw new HubException("Recipient refused");

        var isOnline = connections.Find(metadata.TransferId) is not null;
        if (!isOnline && !deferred.IsEnabled)
            throw new HubException("Recipient is offline");

        if (!parcels.TryOpen(new ParcelKey(sender, metadata.TransferId, metadata.SessionId), metadata, out var parcel, out var refusal))
            throw new HubException(refusal);

        if (isOnline)
            await delivery.AnnounceAsync(parcel);
    }

    /// <summary>
    /// Streams the chunks of one transfer to its recipient, from the first, as they arrive.
    /// </summary>
    /// <param name="metadata">The transfer to read, with the attempt its announcement carried.</param>
    /// <param name="cancellationToken">Stops the stream when the recipient goes away.</param>
    /// <returns>The chunks of the transfer.</returns>
    /// <exception cref="HubException">There is no such transfer for the caller, or it was refused while being read.</exception>
    /// <remarks>
    /// A missing transfer and someone else's answer the same way on purpose: telling an intruder that a transfer
    /// exists but is not theirs is already an answer about other users' traffic.
    /// </remarks>
    public async IAsyncEnumerable<byte[]> StreamBytes(TransferMetadata metadata, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var parcel = parcels.Find(new ParcelKey(metadata.TransferId, Context.UserIdentifier ?? string.Empty, metadata.SessionId));
        if (parcel is null || parcel.Attempt != metadata.Attempt)
            throw new HubException("Transfer not found");

        await foreach (var chunk in parcel.ReadAsync(cancellationToken))
            yield return chunk;
    }

    /// <summary>
    /// Adds one chunk to an open transfer.
    /// </summary>
    /// <param name="chunk">The payload chunk.</param>
    /// <param name="metadata">The transfer the chunk belongs to.</param>
    /// <remarks>
    /// A chunk is a send, which cannot answer. One that crosses a limit refuses its transfer instead, and the
    /// refusal reaches the sender when it completes.
    /// </remarks>
    public Task SendChunk(byte[] chunk, TransferMetadata metadata)
    {
        var parcel = parcels.Find(new ParcelKey(Context.UserIdentifier ?? string.Empty, metadata.TransferId, metadata.SessionId));
        if (parcel is null)
            parcels.CountRefusal();
        else
            parcels.TryAppend(parcel, chunk);

        return Task.CompletedTask;
    }

    /// <summary>
    /// Completes the sending end of a transfer.
    /// </summary>
    /// <param name="metadata">The transfer that finished sending.</param>
    /// <exception cref="HubException">The transfer was refused, is not open, or there is nowhere to keep it for its offline recipient.</exception>
    /// <remarks>
    /// A transfer whose recipient is offline is kept whole from here on, and a recipient that connected while it
    /// was being sent is announced it now.
    /// </remarks>
    public async Task CompleteTransfer(TransferMetadata metadata)
    {
        var parcel = parcels.Find(new ParcelKey(Sender(), metadata.TransferId, metadata.SessionId))
            ?? throw new HubException("Transfer not found");

        if (parcel.Refusal is { } refusal)
        {
            parcels.Remove(parcel);
            throw new HubException(refusal);
        }

        parcel.Seal(clock.GetUtcNow());
        if (parcel.IsAnnounced || await delivery.AnnounceAsync(parcel))
            return;

        var isKept = deferred.TryStore(parcel.ToDeferred(clock.GetUtcNow()));
        parcels.Remove(parcel);

        if (!isKept)
            throw new HubException("The recipient is offline and the relay cannot keep this transfer");
    }

    /// <summary>
    /// Releases a transfer its recipient took.
    /// </summary>
    /// <param name="metadata">The transfer, with the attempt its announcement carried.</param>
    /// <remarks>
    /// Only now is the transfer let go of, and only for the attempt it names: a confirmation of an attempt the
    /// sender has since replaced must not release the new one. A receipt goes out when the sender asked for one.
    /// </remarks>
    public async Task ReceiverCompleted(TransferMetadata metadata)
    {
        if (Received(metadata) is not { } parcel || !parcels.Remove(parcel))
            return;

        if (parcel.IsStored)
            deferred.Commit(parcel.Key.Recipient, parcel.Key.Sender, parcel.Key.Session);

        if (parcel.Metadata.IsAck)
            await delivery.AcknowledgeAsync(parcel.Key);
    }

    /// <summary>
    /// Records that the recipient will not take a transfer: another channel, too large, or its handler failed.
    /// </summary>
    /// <param name="metadata">The transfer, with the attempt its announcement carried.</param>
    /// <remarks>
    /// A refusal used to be a confirmation without acknowledgment, which a transfer that never asked for a receipt
    /// also sends. Its own call tells them apart. A transfer still being sent stays refused until its sender
    /// completes it, so the sender is told; one already complete is simply let go of.
    /// </remarks>
    public Task ReceiverRefused(TransferMetadata metadata)
    {
        if (Received(metadata) is not { } parcel)
            return Task.CompletedTask;

        parcel.Refuse("The recipient refused the transfer");
        parcels.CountRefusal();

        if (parcel.IsSealed && parcels.Remove(parcel) && parcel.IsStored)
            deferred.Commit(parcel.Key.Recipient, parcel.Key.Sender, parcel.Key.Session);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    /// <remarks>
    /// Once a user's last connection drops, every transfer announced to them and not confirmed goes back to the
    /// offline store, so it is announced again on their next connect instead of being lost with this one.
    /// </remarks>
    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        if (Context.UserIdentifier is { Length: > 0 } user)
        {
            connections.Detach(user, Context.ConnectionId);

            if (connections.Find(user) is null)
            {
                foreach (var parcel in parcels.For(user).Where(parcel => parcel.IsAnnounced))
                    delivery.Return(parcel);
            }

            logger.LogDebug("Disconnected {User}", redactor.Name(user));
        }

        await base.OnDisconnectedAsync(exception);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Connecting only records the connection and queues an announcement. Pushing waiting transfers here would hold
    /// the SignalR handshake open for as long as a whole caching window of traffic takes to send.
    /// </remarks>
    public override async Task OnConnectedAsync()
    {
        if (Context.UserIdentifier is { Length: > 0 } user)
        {
            connections.Attach(user, Context.ConnectionId);
            backlog.Enqueue(user);
            logger.LogDebug("Connected {User}", redactor.Name(user));
        }

        await base.OnConnectedAsync();
    }

    private Parcel? Received(TransferMetadata metadata) =>
        parcels.Find(new ParcelKey(metadata.TransferId, Context.UserIdentifier ?? string.Empty, metadata.SessionId)) is { } parcel
        && parcel.Attempt == metadata.Attempt
            ? parcel
            : null;

    private string Sender() =>
        Context.UserIdentifier is { Length: > 0 } user ? user : throw new HubException("Not signed in");

    private bool IsValid(TransferMetadata metadata)
    {
        var limit = limits.Value.MaxIdLength;

        return metadata.TransferId is { Length: > 0 } recipient && recipient.Length <= limit
            && metadata.SessionId is { Length: > 0 } session && session.Length <= limit
            && metadata.Channel is { Length: > 0 } channel && channel.Length <= limit;
    }
}
