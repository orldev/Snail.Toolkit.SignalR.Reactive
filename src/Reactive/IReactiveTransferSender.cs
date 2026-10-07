using Microsoft.AspNetCore.SignalR;
using Snail.Toolkit.SignalR.Reactive.Transfers;

namespace Snail.Toolkit.SignalR.Reactive;

/// <summary>
/// Sends chunked binary transfers to another client.
/// </summary>
public interface IReactiveTransferSender : IAsyncDisposable
{
    /// <summary>
    /// Raised with the session id when a recipient confirms it received a transfer.
    /// </summary>
    /// <remarks>
    /// Only transfers sent with acknowledgment enabled ever raise this, and a receipt buffered while the
    /// sender was offline arrives on the next connect rather than being lost.
    /// </remarks>
    event Func<string, Task>? OnAcknowledgeReceipted;

    /// <summary>
    /// Raised with the recipient and the session when a recipient confirms it received a transfer.
    /// </summary>
    /// <remarks>
    /// The counterpart of <see cref="OnAcknowledgeReceipted"/> that names the recipient: a sender that writes one
    /// session to several recipients needs to know which of them confirmed.
    /// </remarks>
    event Func<TransferReceipt, Task>? Acknowledged;

    /// <summary>
    /// Splits a payload into chunks and sends it.
    /// </summary>
    /// <param name="targetClientId">The recipient.</param>
    /// <param name="bytes">The payload.</param>
    /// <param name="chunkSize">Bytes per chunk.</param>
    /// <param name="sessionId">A session id to reuse; a new one is generated when omitted.</param>
    /// <param name="options">The channel and acknowledgment for this transfer.</param>
    /// <returns>A task that completes once the hub has accepted the whole transfer.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="chunkSize"/> is not positive.</exception>
    /// <exception cref="HubException">The hub refused the transfer or has nowhere to keep it.</exception>
    /// <remarks>
    /// Suitable for payloads that already fit in memory; the chunk count is derived arithmetically rather
    /// than by walking the data.
    /// </remarks>
    Task SendAsync(string targetClientId, byte[] bytes, int chunkSize = 8192, string? sessionId = null,
        TransferOptions? options = null);

    /// <summary>
    /// Sends a stream of chunks as one transfer.
    /// </summary>
    /// <param name="targetClientId">The recipient.</param>
    /// <param name="dataStream">The chunks to send, in order.</param>
    /// <param name="sessionId">A session id to reuse; a new one is generated when omitted.</param>
    /// <param name="options">The channel and acknowledgment for this transfer.</param>
    /// <returns>A task that completes once the stream ended and the hub accepted the transfer.</returns>
    /// <exception cref="HubException">The hub refused the transfer or has nowhere to keep it.</exception>
    /// <remarks>
    /// The stream is subscribed exactly once and its length is never probed. Counting it up front read a
    /// cold source twice and never returned at all for a hot one, which is the case this overload exists
    /// for. The hub holds every chunk until the recipient confirms, so nothing has to be sized up front.
    /// </remarks>
    Task SendAsync(string targetClientId, IObservable<byte[]> dataStream, string? sessionId = null,
        TransferOptions? options = null);

}
