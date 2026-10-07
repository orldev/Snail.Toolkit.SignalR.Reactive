using System.Threading.Channels;

namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// The users whose buffered transfers still have to be replayed.
/// </summary>
/// <remarks>
/// Replaying used to happen inside <c>OnConnectedAsync</c>, which held the SignalR handshake open until
/// every buffered byte of a whole caching window had been pushed. Connecting now only leaves a name here.
/// </remarks>
public sealed class TransferBacklog
{
    private readonly Channel<string> _pending = Channel.CreateUnbounded<string>(new UnboundedChannelOptions
    {
        SingleReader = true
    });

    /// <summary>
    /// Notes that a user connected and may have buffered transfers waiting.
    /// </summary>
    /// <param name="userId">The user that connected.</param>
    public void Enqueue(string userId) => _pending.Writer.TryWrite(userId);

    /// <summary>
    /// Reads connected users as they arrive.
    /// </summary>
    /// <param name="ct">Stops the enumeration when the host shuts down.</param>
    /// <returns>The users to replay for.</returns>
    public IAsyncEnumerable<string> ReadAllAsync(CancellationToken ct) => _pending.Reader.ReadAllAsync(ct);
}
