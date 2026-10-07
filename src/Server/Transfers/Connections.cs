using System.Collections.Immutable;

namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// The connections each user is currently reachable on, for one server node.
/// </summary>
/// <remarks>
/// A user may hold several connections — two devices, or a reconnect whose new connect arrives before the old
/// disconnect — and transfers go to the newest. Keeping only one used to let a second device take every transfer
/// from the first, and detaching only removes the connection that actually dropped: SignalR discards a send to a
/// stale connection id without raising anything, so a wrong entry here reads as a transfer that never arrives.
/// </remarks>
public sealed class Connections : IConnections
{
    private readonly ConcurrentDictionary<string, ImmutableList<string>> _byUser = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public void Attach(string userId, string connectionId) =>
        _byUser.AddOrUpdate(userId, [connectionId], (_, known) => known.Remove(connectionId).Add(connectionId));

    /// <inheritdoc />
    public bool Detach(string userId, string connectionId)
    {
        while (_byUser.TryGetValue(userId, out var known))
        {
            if (!known.Contains(connectionId))
                return false;

            var remaining = known.Remove(connectionId);
            var isDone = remaining.IsEmpty
                ? _byUser.TryRemove(new KeyValuePair<string, ImmutableList<string>>(userId, known))
                : _byUser.TryUpdate(userId, remaining, known);

            if (isDone)
                return true;
        }

        return false;
    }

    /// <inheritdoc />
    public string? Find(string userId) => _byUser.TryGetValue(userId, out var known) && !known.IsEmpty ? known[^1] : null;
}
