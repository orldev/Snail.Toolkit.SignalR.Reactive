using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Options;

namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// The transfers this node holds right now: being sent, being read, or waiting for their recipient's confirmation.
/// </summary>
/// <param name="limits">How much one sender and the node as a whole may hold.</param>
/// <param name="clock">The clock activity is measured by.</param>
/// <remarks>
/// One dictionary keyed by sender, recipient and session, so opening, finding and removing are each a single atomic
/// call. The byte total is carried rather than summed, which keeps the node-wide limit a constant-time check.
/// </remarks>
public sealed class Parcels(IOptions<TransferLimitOptions> limits, TimeProvider clock)
{
    private readonly ConcurrentDictionary<ParcelKey, Parcel> _parcels = new();

    private readonly TransferLimitOptions _limits = limits.Value;

    private long _held;

    private long _refused;

    /// <summary>
    /// Gets how many payload bytes are held in all.
    /// </summary>
    public long HeldBytes => Interlocked.Read(ref _held);

    /// <summary>
    /// Opens a transfer, replacing an earlier attempt under the same key.
    /// </summary>
    /// <param name="key">Who sends, who receives, and the session.</param>
    /// <param name="metadata">The metadata the sender opened it with.</param>
    /// <param name="parcel">The transfer, when it was opened.</param>
    /// <param name="refusal">Why it could not be opened, when it was not.</param>
    /// <returns><c>true</c> when the transfer was opened.</returns>
    /// <remarks>
    /// A sender that opens a session again is retrying: the earlier attempt is refused, so a recipient still reading
    /// it fails instead of being handed half of one attempt and half of the next.
    /// </remarks>
    public bool TryOpen(ParcelKey key, TransferMetadata metadata, [NotNullWhen(true)] out Parcel? parcel, [NotNullWhen(false)] out string? refusal)
    {
        parcel = null;
        refusal = null;

        if (HeldBytes >= _limits.MaxHeldBytes)
        {
            refusal = Refuse("The relay is holding all it can; try again later");
            return false;
        }

        if (!_parcels.ContainsKey(key) && _parcels.Keys.Count(other => other.Sender == key.Sender) >= _limits.MaxOpenTransfersPerSender)
        {
            refusal = Refuse("Too many transfers open at once");
            return false;
        }

        var opened = new Parcel(key, metadata, clock.GetUtcNow());
        while (true)
        {
            if (_parcels.TryGetValue(key, out var earlier))
            {
                if (!_parcels.TryUpdate(key, opened, earlier))
                    continue;

                earlier.Refuse("The sender started this transfer again");
                Interlocked.Add(ref _held, -earlier.Bytes);
                break;
            }

            if (_parcels.TryAdd(key, opened))
                break;
        }

        parcel = opened;
        return true;
    }

    /// <summary>
    /// Takes up a transfer the offline store kept, so it can be announced to its recipient.
    /// </summary>
    /// <param name="stored">The transfer as it was stored.</param>
    /// <returns>The transfer, or <c>null</c> when one with the same key is already held.</returns>
    public Parcel? Adopt(DeferredSession stored)
    {
        var parcel = Parcel.From(stored, clock.GetUtcNow());
        if (!_parcels.TryAdd(parcel.Key, parcel))
            return null;

        Interlocked.Add(ref _held, parcel.Bytes);
        return parcel;
    }

    /// <summary>
    /// Adds a chunk, refusing the transfer when it would cross a limit.
    /// </summary>
    /// <param name="parcel">The transfer the chunk belongs to.</param>
    /// <param name="chunk">The chunk.</param>
    /// <returns><c>true</c> when the chunk was taken.</returns>
    /// <remarks>
    /// A chunk arrives as a send, which cannot answer the sender. Refusing the transfer instead is what makes the
    /// completing call fail, so the sender learns the payload did not get through rather than that it did.
    /// </remarks>
    public bool TryAppend(Parcel parcel, byte[] chunk)
    {
        if (parcel.Bytes + chunk.Length > _limits.MaxTransferBytes)
        {
            parcel.Refuse("The transfer is larger than this relay accepts");
            Interlocked.Increment(ref _refused);
            return false;
        }

        if (HeldBytes + chunk.Length > _limits.MaxHeldBytes)
        {
            parcel.Refuse("The relay is holding all it can; try again later");
            Interlocked.Increment(ref _refused);
            return false;
        }

        if (!parcel.TryAppend(chunk, clock.GetUtcNow()))
            return false;

        Interlocked.Add(ref _held, chunk.Length);
        return true;
    }

    /// <summary>
    /// Finds a transfer by its key.
    /// </summary>
    /// <param name="key">Who sends, who receives, and the session.</param>
    /// <returns>The transfer, or <c>null</c> when none is held.</returns>
    public Parcel? Find(ParcelKey key) => _parcels.GetValueOrDefault(key);

    /// <summary>
    /// Stops holding a transfer, unless it was replaced by a newer attempt meanwhile.
    /// </summary>
    /// <param name="parcel">The transfer to let go of.</param>
    /// <returns><c>true</c> when this was the transfer held and it is gone.</returns>
    public bool Remove(Parcel parcel)
    {
        if (!_parcels.TryRemove(new KeyValuePair<ParcelKey, Parcel>(parcel.Key, parcel)))
            return false;

        Interlocked.Add(ref _held, -parcel.Bytes);
        return true;
    }

    /// <summary>
    /// Lists the transfers waiting for one recipient, oldest first.
    /// </summary>
    /// <param name="recipient">The recipient.</param>
    /// <returns>The transfers.</returns>
    public IReadOnlyList<Parcel> For(string recipient) =>
        [.. _parcels.Values.Where(parcel => parcel.Key.Recipient == recipient).OrderBy(parcel => parcel.OpenedAt)];

    /// <summary>
    /// Lists every transfer held.
    /// </summary>
    /// <returns>The transfers.</returns>
    public IReadOnlyList<Parcel> All() => [.. _parcels.Values];

    /// <summary>
    /// Counts a transfer refused somewhere the sender could not be told at once.
    /// </summary>
    public void CountRefusal() => Interlocked.Increment(ref _refused);

    /// <summary>
    /// Takes the number of refusals since it was last taken.
    /// </summary>
    /// <returns>The count.</returns>
    public long TakeRefusals() => Interlocked.Exchange(ref _refused, 0);

    private string Refuse(string reason)
    {
        Interlocked.Increment(ref _refused);
        return reason;
    }
}
