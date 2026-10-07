using Microsoft.Extensions.Options;

namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// Whole transfers and receipts kept in this node's memory for parties that are not connected.
/// </summary>
/// <param name="options">How long and how much to keep.</param>
/// <param name="clock">The clock expiry is measured by.</param>
/// <remarks>
/// Keeping is off unless a duration is configured, so a deployment that has not opted in keeps nothing in memory.
/// Every operation takes one lock: the earlier check-then-set store let two senders to one offline recipient
/// replace each other's buffer, and a chunk added between a drain's emptiness check and its removal vanished.
/// Contention is bounded by how often transfers complete, not by how many chunks they carry.
/// </remarks>
public sealed class DeferredTransfers(IOptions<TransferCacheOptions> options, TimeProvider clock) : IDeferredTransfers
{
    private readonly TransferCacheOptions _options = options.Value;

    private readonly Lock _gate = new();

    private readonly Dictionary<string, List<DeferredSession>> _sessions = new(StringComparer.Ordinal);

    private readonly Dictionary<string, List<(TransferReceipt Receipt, DateTimeOffset StoredAt)>> _receipts = new(StringComparer.Ordinal);

    private long _bytes;

    /// <inheritdoc />
    public bool IsEnabled => _options.PendingTransferCacheDuration > 0;

    private TimeSpan Lifetime => TimeSpan.FromHours(_options.PendingTransferCacheDuration);

    /// <inheritdoc />
    /// <remarks>
    /// Three limits apply: what one recipient may be kept, the share of it one sender may fill, and what the node
    /// keeps for everyone. A replaced transfer frees its bytes before the new one is weighed.
    /// </remarks>
    public bool TryStore(DeferredSession session)
    {
        if (!IsEnabled)
            return false;

        lock (_gate)
        {
            SweepLocked(clock.GetUtcNow());

            var kept = _sessions.TryGetValue(session.RecipientId, out var existing) ? existing : [];
            var replaced = kept.FirstOrDefault(other => other.SenderId == session.SenderId && other.SessionId == session.SessionId);
            var replacedBytes = replaced?.Bytes ?? 0;

            var recipientBytes = kept.Sum(other => other.Bytes) - replacedBytes + session.Bytes;
            var senderBytes = kept.Where(other => other.SenderId == session.SenderId).Sum(other => other.Bytes) - replacedBytes + session.Bytes;
            var totalBytes = _bytes - replacedBytes + session.Bytes;

            if (_options.PendingTransferMaxBytes > 0 && recipientBytes > _options.PendingTransferMaxBytes)
                return false;

            if (_options.PendingTransferMaxBytes > 0 && senderBytes > _options.PendingTransferMaxBytes * _options.PendingTransferSenderShare)
                return false;

            if (_options.PendingTransferTotalBytes > 0 && totalBytes > _options.PendingTransferTotalBytes)
                return false;

            if (replaced is not null)
                kept.Remove(replaced);

            kept.Add(session);
            _sessions[session.RecipientId] = kept;
            _bytes = totalBytes;

            return true;
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<DeferredSession> Peek(string recipientId)
    {
        lock (_gate)
        {
            SweepLocked(clock.GetUtcNow());

            return _sessions.TryGetValue(recipientId, out var kept) ? [.. kept] : [];
        }
    }

    /// <inheritdoc />
    public void Commit(string recipientId, string senderId, string sessionId)
    {
        lock (_gate)
        {
            if (!_sessions.TryGetValue(recipientId, out var kept))
                return;

            var committed = kept.FirstOrDefault(session => session.SenderId == senderId && session.SessionId == sessionId);
            if (committed is null)
                return;

            kept.Remove(committed);
            _bytes -= committed.Bytes;

            if (kept.Count == 0)
                _sessions.Remove(recipientId);
        }
    }

    /// <inheritdoc />
    public void StoreReceipt(string senderId, TransferReceipt receipt)
    {
        if (!IsEnabled)
            return;

        lock (_gate)
        {
            if (!_receipts.TryGetValue(senderId, out var kept))
                _receipts[senderId] = kept = [];

            kept.Add((receipt, clock.GetUtcNow()));
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<TransferReceipt> DrainReceipts(string senderId)
    {
        lock (_gate)
        {
            SweepLocked(clock.GetUtcNow());

            if (!_receipts.Remove(senderId, out var kept))
                return [];

            return [.. kept.Select(entry => entry.Receipt)];
        }
    }

    /// <inheritdoc />
    public void Sweep()
    {
        lock (_gate)
        {
            SweepLocked(clock.GetUtcNow());
        }
    }

    private void SweepLocked(DateTimeOffset now)
    {
        foreach (var recipient in _sessions.Keys.ToList())
        {
            var kept = _sessions[recipient];
            foreach (var expired in kept.Where(session => session.StoredAt + Lifetime <= now).ToList())
            {
                kept.Remove(expired);
                _bytes -= expired.Bytes;
            }

            if (kept.Count == 0)
                _sessions.Remove(recipient);
        }

        foreach (var sender in _receipts.Keys.ToList())
        {
            _receipts[sender].RemoveAll(entry => entry.StoredAt + Lifetime <= now);
            if (_receipts[sender].Count == 0)
                _receipts.Remove(sender);
        }
    }
}
