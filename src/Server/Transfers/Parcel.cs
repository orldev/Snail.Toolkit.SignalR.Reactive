using System.Runtime.CompilerServices;

namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// One transfer as the hub holds it: every chunk the sender sent, until the recipient confirms it took them.
/// </summary>
/// <remarks>
/// The chunks are kept for the whole life of the transfer rather than in a replay window, so a recipient that
/// subscribes late, drops mid-way or crashes before confirming is handed the payload again from its first byte.
/// Delivery is therefore at least once; a recipient that must not act twice deduplicates by session id.
/// <para>
/// Each opening gets a fresh <see cref="Attempt"/>. The recipient echoes it back, which is what keeps the stream or
/// the confirmation of an attempt the sender has already replaced from touching the new one.
/// </para>
/// </remarks>
public sealed class Parcel
{
    private readonly List<byte[]> _chunks = [];

    private readonly Lock _gate = new();

    private TaskCompletionSource _changed = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Starts holding a transfer the sender just opened.
    /// </summary>
    /// <param name="key">Who sends, who receives, and the session.</param>
    /// <param name="metadata">The channel and acknowledgment the sender asked for.</param>
    /// <param name="now">The moment it opened.</param>
    public Parcel(ParcelKey key, TransferMetadata metadata, DateTimeOffset now)
    {
        Key = key;
        Metadata = metadata;
        OpenedAt = now;
        LastActivity = now;
    }

    /// <summary>
    /// Gets who sends, who receives, and the session.
    /// </summary>
    public ParcelKey Key { get; }

    /// <summary>
    /// Gets the metadata the sender opened the transfer with.
    /// </summary>
    public TransferMetadata Metadata { get; }

    /// <summary>
    /// Gets the id of this opening, which the recipient echoes back.
    /// </summary>
    public string Attempt { get; } = Guid.NewGuid().ToString("N");

    /// <summary>
    /// Gets when the transfer was opened.
    /// </summary>
    public DateTimeOffset OpenedAt { get; }

    /// <summary>
    /// Gets when the sender last added to it, or when it was last announced.
    /// </summary>
    public DateTimeOffset LastActivity { get; private set; }

    /// <summary>
    /// Gets when the recipient was told about it, or <c>null</c> while it has not been.
    /// </summary>
    public DateTimeOffset? AnnouncedAt { get; private set; }

    /// <summary>
    /// Gets how many payload bytes it holds.
    /// </summary>
    public long Bytes { get; private set; }

    /// <summary>
    /// Gets a value indicating whether the sender completed it.
    /// </summary>
    public bool IsSealed { get; private set; }

    /// <summary>
    /// Gets why the transfer was refused, or <c>null</c> while it stands.
    /// </summary>
    public string? Refusal { get; private set; }

    /// <summary>
    /// Gets a value indicating whether a copy of it already sits in the offline store.
    /// </summary>
    public bool IsStored { get; init; }

    /// <summary>
    /// Gets a value indicating whether the recipient has been told about it.
    /// </summary>
    public bool IsAnnounced => AnnouncedAt is not null;

    /// <summary>
    /// Gets the chunks held so far, oldest first.
    /// </summary>
    public IReadOnlyList<byte[]> Chunks
    {
        get
        {
            lock (_gate)
            {
                return [.. _chunks];
            }
        }
    }

    /// <summary>
    /// Builds a sealed parcel from a transfer the offline store kept.
    /// </summary>
    /// <param name="stored">The transfer as it was stored.</param>
    /// <param name="now">The moment it is taken up again.</param>
    /// <returns>A sealed parcel that knows it is stored.</returns>
    public static Parcel From(DeferredSession stored, DateTimeOffset now)
    {
        var metadata = new TransferMetadata(stored.RecipientId, stored.SessionId, stored.Channel, stored.IsAck);
        var parcel = new Parcel(new ParcelKey(stored.SenderId, stored.RecipientId, stored.SessionId), metadata, now) { IsStored = true };

        foreach (var chunk in stored.Chunks)
            parcel.TryAppend(chunk, now);

        parcel.Seal(now);

        return parcel;
    }

    /// <summary>
    /// Gets the transfer as the offline store keeps it.
    /// </summary>
    /// <param name="now">The moment it is stored.</param>
    /// <returns>The sealed transfer.</returns>
    public DeferredSession ToDeferred(DateTimeOffset now) =>
        new(Key.Sender, Key.Recipient, Key.Session, Metadata.Channel, Metadata.IsAck, Chunks, now);

    /// <summary>
    /// Adds a chunk the sender sent, unless the transfer is refused or complete.
    /// </summary>
    /// <param name="chunk">The chunk.</param>
    /// <param name="now">The moment it arrived.</param>
    /// <returns><c>true</c> when the chunk was added.</returns>
    /// <remarks>The check and the add share one lock, so no chunk slips in after a refusal has been counted.</remarks>
    public bool TryAppend(byte[] chunk, DateTimeOffset now)
    {
        var isAdded = false;
        Change(() =>
        {
            if (Refusal is not null || IsSealed)
                return;

            _chunks.Add(chunk);
            Bytes += chunk.Length;
            LastActivity = now;
            isAdded = true;
        });

        return isAdded;
    }

    /// <summary>
    /// Marks the transfer complete: no more chunks follow.
    /// </summary>
    /// <param name="now">The moment the sender completed it.</param>
    public void Seal(DateTimeOffset now) => Change(() =>
    {
        IsSealed = true;
        LastActivity = now;
    });

    /// <summary>
    /// Refuses the transfer: whoever reads it fails, and completing it fails.
    /// </summary>
    /// <param name="reason">Why, in words the sender is shown.</param>
    public void Refuse(string reason) => Change(() => Refusal ??= reason);

    /// <summary>
    /// Records that the recipient was told about it.
    /// </summary>
    /// <param name="now">The moment of the announcement.</param>
    public void Announce(DateTimeOffset now) => Change(() =>
    {
        AnnouncedAt = now;
        LastActivity = now;
    });

    /// <summary>
    /// Forgets the announcement, so the next connect of the recipient announces it again.
    /// </summary>
    public void Withdraw() => Change(() => AnnouncedAt = null);

    /// <summary>
    /// Streams the chunks to the recipient: those held already, then each one as it arrives, until the sender completes.
    /// </summary>
    /// <param name="cancellationToken">Stops the stream when the recipient goes away.</param>
    /// <returns>Every chunk of the transfer, from the first.</returns>
    /// <exception cref="HubException">The transfer was refused while it was being read.</exception>
    public async IAsyncEnumerable<byte[]> ReadAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var next = 0;

        while (true)
        {
            byte[][] ready;
            Task changed;
            bool isDone;

            lock (_gate)
            {
                if (Refusal is not null)
                    throw new HubException(Refusal);

                ready = [.. _chunks.Skip(next)];
                next = _chunks.Count;
                isDone = IsSealed;
                changed = _changed.Task;
            }

            foreach (var chunk in ready)
                yield return chunk;

            if (isDone)
                yield break;

            await changed.WaitAsync(cancellationToken);
        }
    }

    private void Change(Action change)
    {
        TaskCompletionSource changed;

        lock (_gate)
        {
            change();
            changed = _changed;
            _changed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        changed.TrySetResult();
    }
}
