namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// Where each user can be reached right now.
/// </summary>
/// <remarks>
/// The shipped implementation is node-local, which is what makes a multi-node deployment need a shared one:
/// a SignalR backplane forwards messages between nodes, but it does not tell one node which connection
/// another node is holding.
/// </remarks>
public interface IConnections
{
    /// <summary>
    /// Records the connection a user is reachable on.
    /// </summary>
    /// <param name="userId">The connected user.</param>
    /// <param name="connectionId">The connection to reach them on.</param>
    void Attach(string userId, string connectionId);

    /// <summary>
    /// Forgets one connection of a user, leaving any other they hold.
    /// </summary>
    /// <param name="userId">The disconnecting user.</param>
    /// <param name="connectionId">The connection that dropped.</param>
    /// <returns><c>true</c> when the mapping was removed.</returns>
    bool Detach(string userId, string connectionId);

    /// <summary>
    /// Finds the connection a user is reachable on: the newest, when there are several.
    /// </summary>
    /// <param name="userId">The user to reach.</param>
    /// <returns>The connection id, or <c>null</c> when the user is offline.</returns>
    string? Find(string userId);
}
