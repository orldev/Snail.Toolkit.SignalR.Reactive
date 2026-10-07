using System.Reactive.Disposables;
using System.Reactive.Threading.Tasks;
using Snail.Toolkit.SignalR.Reactive.Transfers;

namespace Snail.Toolkit.SignalR.Reactive;

/// <summary>
/// Sends chunked binary transfers over a SignalR connection.
/// </summary>
/// <remarks>
/// Registered as a singleton and therefore stateless between calls: everything a transfer needs travels in
/// its arguments, so one caller's channel or acknowledgment choice cannot leak into another's.
/// </remarks>
public sealed class ReactiveTransferSender : IReactiveTransferSender
{
    private readonly HubConnection _hubConnection;
    private readonly ILogger<ReactiveTransferSender> _logger;
    private readonly CompositeDisposable _disposables = new();

    /// <inheritdoc />
    public event Func<string, Task>? OnAcknowledgeReceipted;

    /// <inheritdoc />
    public event Func<TransferReceipt, Task>? Acknowledged;

    /// <summary>
    /// Attaches to a started hub connection and listens for delivery receipts.
    /// </summary>
    /// <param name="hubConnection">The connection to send over; it must be started before use.</param>
    /// <param name="logger">The logger for transfer failures.</param>
    /// <remarks>
    /// The receipt handler registration is kept so that disposing the sender detaches it: leaving it
    /// attached kept this instance alive through the connection for the life of the process.
    /// </remarks>
    public ReactiveTransferSender(HubConnection hubConnection, ILogger<ReactiveTransferSender> logger)
    {
        _hubConnection = hubConnection;
        _logger = logger;

        _disposables.Add(_hubConnection.On<TransferMetadata>(nameof(IReactiveTransferClient.AcknowledgeReceipt), async metadata =>
        {
            logger.LogDebug("Receipt for session {SessionId}", metadata.SessionId);

            var receipted = OnAcknowledgeReceipted;
            if (receipted is not null)
                await receipted(metadata.SessionId);

            var acknowledged = Acknowledged;
            if (acknowledged is not null)
                await acknowledged(new TransferReceipt(metadata.TransferId, metadata.SessionId));
        }));
    }

    /// <inheritdoc />
    public async Task SendAsync(string targetClientId, byte[] bytes, int chunkSize = 8192, string? sessionId = null,
        TransferOptions? options = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(chunkSize, 0);

        options ??= TransferOptions.Default;
        var metadata = new TransferMetadata(targetClientId, sessionId ?? Guid.NewGuid().ToString(), options.Channel, options.IsAck);

        await SendCoreAsync(metadata, CreateObservable(bytes, chunkSize));
    }

    /// <inheritdoc />
    public async Task SendAsync(string targetClientId, IObservable<byte[]> dataStream, string? sessionId = null,
        TransferOptions? options = null)
    {
        options ??= TransferOptions.Default;
        var metadata = new TransferMetadata(targetClientId, sessionId ?? Guid.NewGuid().ToString(), options.Channel, options.IsAck);

        await SendCoreAsync(metadata, dataStream);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _disposables.Dispose();

        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Opens the transfer, pumps every chunk in order, then closes it.
    /// </summary>
    /// <param name="metadata">The transfer being sent.</param>
    /// <param name="dataStream">The chunks to send.</param>
    /// <remarks>
    /// The pipeline is awaited directly rather than bridged through a subscription: <c>async</c> subscribe
    /// handlers compile to <c>async void</c>, so a failed completion notification tore the process down
    /// instead of surfacing to the caller.
    /// <para>
    /// Opening and closing are invocations, not sends, so the returned task means the hub accepted the
    /// whole transfer rather than that the bytes merely left the client: completing fails with a
    /// <see cref="HubException"/> whenever the hub refused or could not keep it. Chunks stay sends and rely on the hub
    /// running one invocation per client at a time, which is the SignalR default; raising
    /// <c>MaximumParallelInvocationsPerClient</c> reorders them.
    /// </para>
    /// </remarks>
    private async Task SendCoreAsync(TransferMetadata metadata, IObservable<byte[]> dataStream)
    {
        try
        {
            await _hubConnection.InvokeAsync(nameof(IReactiveTransferHub.InitiateTransfer), metadata);

            await dataStream
                .Select(chunk => Observable.FromAsync(ct =>
                    _hubConnection.SendAsync(nameof(IReactiveTransferHub.SendChunk), chunk, metadata, ct)))
                .Concat()
                .DefaultIfEmpty()
                .ToTask();

            await _hubConnection.InvokeAsync(nameof(IReactiveTransferHub.CompleteTransfer), metadata);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Transfer {SessionId} failed", metadata.SessionId);

            throw;
        }
    }

    /// <summary>
    /// Splits a payload into chunks as a cold sequence.
    /// </summary>
    /// <param name="bytes">The payload to split.</param>
    /// <param name="chunkSize">Bytes per chunk.</param>
    /// <returns>The chunks, every one but the last exactly <paramref name="chunkSize"/> bytes.</returns>
    private static IObservable<byte[]> CreateObservable(byte[] bytes, int chunkSize) =>
        Observable.Create<byte[]>(observer =>
        {
            try
            {
                for (var offset = 0; offset < bytes.Length; offset += chunkSize)
                {
                    var currentChunkSize = Math.Min(chunkSize, bytes.Length - offset);
                    var chunk = new byte[currentChunkSize];
                    Buffer.BlockCopy(bytes, offset, chunk, 0, currentChunkSize);

                    observer.OnNext(chunk);
                }

                observer.OnCompleted();
            }
            catch (Exception ex)
            {
                observer.OnError(ex);
            }

            return Disposable.Empty;
        });
}
