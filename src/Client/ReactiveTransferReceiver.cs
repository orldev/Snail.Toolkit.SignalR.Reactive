using System.Reactive.Disposables;
using Snail.Toolkit.SignalR.Reactive.Transfers;

namespace Snail.Toolkit.SignalR.Reactive;

/// <summary>
/// Receives chunked binary transfers over a SignalR connection.
/// </summary>
/// <remarks>
/// Each session keeps its own destination stream, cancellation source and subscription, all keyed by
/// session id, so concurrent transfers never touch each other's state.
/// </remarks>
public sealed class ReactiveTransferReceiver : IReactiveTransferReceiver
{
    private volatile string _currentChannel = "default";
    private readonly HubConnection _hubConnection;
    private readonly ILogger<ReactiveTransferReceiver> _logger;
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _ctsMap = new();
    private readonly ConcurrentDictionary<string, IDisposable> _subscriptions = new();
    private readonly ConcurrentDictionary<string, Stream> _destinations = new();
    private readonly ConcurrentDictionary<string, long> _received = new();
    private readonly Queue<TransferMetadata> _waiting = new();
    private readonly Lock _admission = new();
    private int _active;

    /// <summary>
    /// How many announced transfers may wait for a free place before more are refused.
    /// </summary>
    /// <remarks>
    /// A waiting transfer costs this device only its metadata — its chunks stay on the hub until it is streamed —
    /// so the queue can be long; it is bounded all the same, because the sender decides how many there are.
    /// </remarks>
    public const int MaxWaitingTransfers = 1024;

    /// <inheritdoc />
    public long MaxTransferBytes { get; set; } = 64L * 1024 * 1024;

    /// <inheritdoc />
    public int MaxConcurrentTransfers { get; set; } = 16;

    /// <inheritdoc />
    public Func<string, string, Task<Stream>>? OpenDestination { get; set; }

    /// <inheritdoc />
    public event Func<string, byte[], Task>? TransferCompleted;

    /// <inheritdoc />
    public event Func<string, Task>? TransferStored;

    /// <inheritdoc />
    public event Func<string, byte[], Task>? OnChunkReceived;

    /// <summary>
    /// Attaches to a hub connection and starts accepting transfers announced on it.
    /// </summary>
    /// <param name="hubConnection">The connection to receive over.</param>
    /// <param name="logger">The logger for transfer failures.</param>
    /// <remarks>
    /// The handler must be attached before the connection starts: the hub announces buffered transfers
    /// during the connect call itself, and an announcement with no handler is dropped by SignalR.
    /// </remarks>
    public ReactiveTransferReceiver(HubConnection hubConnection, ILogger<ReactiveTransferReceiver> logger)
    {
        _hubConnection = hubConnection;
        _logger = logger;

        _hubConnection.On<TransferMetadata>(nameof(IReactiveTransferClient.PrepareForTransfer), async metadata =>
        {
            _logger.LogDebug("Transfer {SessionId} announced", metadata.SessionId);

            await AdmitAsync(metadata);
        });
    }

    /// <inheritdoc />
    public void SetChannel(string channel)
    {
        _currentChannel = channel;
        _logger.LogDebug("Receiver now listening to channel: {Channel}", _currentChannel);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        foreach (var cts in _ctsMap.Values)
        {
            await cts.CancelAsync();
            cts.Dispose();
        }

        _ctsMap.Clear();

        foreach (var subscription in _subscriptions.Values)
        {
            subscription.Dispose();
        }

        _subscriptions.Clear();

        foreach (var destination in _destinations.Values)
        {
            await destination.DisposeAsync();
        }

        _destinations.Clear();
    }

    /// <summary>
    /// Accepts an announced transfer and starts draining it.
    /// </summary>
    /// <param name="metadata">The announced transfer.</param>
    /// <remarks>
    /// A transfer on another channel is refused back to the hub rather than ignored: the hub already holds
    /// a session with the whole payload in it, and nothing else would ever release it.
    /// <para>
    /// Beyond <see cref="MaxConcurrentTransfers"/> a transfer waits for a place instead of being refused. Refusing
    /// was measured to lose call signalling: a call sends a burst of small letters at once, and the ones past the
    /// limit never arrived. Only beyond <see cref="MaxWaitingTransfers"/> is one refused.
    /// </para>
    /// </remarks>
    private async Task AdmitAsync(TransferMetadata metadata)
    {
        if (metadata.Channel != _currentChannel)
        {
            _logger.LogDebug("Refusing transfer {SessionId} on channel {TransferChannel}", metadata.SessionId, metadata.Channel);
            await RefuseAsync(metadata);

            return;
        }

        if (_destinations.ContainsKey(KeyOf(metadata)))
            return;

        bool isStarting;
        lock (_admission)
        {
            isStarting = _active < MaxConcurrentTransfers;
            if (isStarting)
                _active++;
            else if (_waiting.Count < MaxWaitingTransfers)
            {
                _waiting.Enqueue(metadata);
                return;
            }
        }

        if (!isStarting)
        {
            await RefuseAsync(metadata);
            return;
        }

        await StartAndSubscribeTransfer(metadata);
    }

    /// <summary>
    /// Gives a finished transfer's place to the next one waiting, or frees it.
    /// </summary>
    private void ReleaseSlot()
    {
        TransferMetadata? next;
        lock (_admission)
        {
            if (!_waiting.TryDequeue(out next))
                _active--;
        }

        if (next is not null)
            _ = StartAndSubscribeTransfer(next);
    }

    /// <summary>
    /// Opens the destination of an admitted transfer and starts draining it.
    /// </summary>
    /// <param name="metadata">The admitted transfer, which holds one place.</param>
    /// <remarks>
    /// Started both from an announcement and from the queue, where nobody awaits it, so every failure is handled
    /// here: the place is given back and the transfer refused.
    /// <para>
    /// The buffer is claimed with a single atomic add, which is also what makes a duplicate announcement for
    /// the same session harmless.
    /// </para>
    /// </remarks>
    private async Task StartAndSubscribeTransfer(TransferMetadata metadata)
    {
        var key = KeyOf(metadata);
        var opening = OpenDestination;
        Stream destination;

        try
        {
            destination = opening is null
                ? new MemoryStream(capacity: 1024)
                : await opening(metadata.TransferId, metadata.SessionId);
        }
        catch (Exception failure)
        {
            _logger.LogDebug(failure, "Could not open a destination for transfer {SessionId}", metadata.SessionId);
            ReleaseSlot();
            await RefuseAsync(metadata);

            return;
        }

        if (!_destinations.TryAdd(key, destination))
        {
            await destination.DisposeAsync();
            ReleaseSlot();

            return;
        }

        var subscription = new SingleAssignmentDisposable();
        _subscriptions[key] = subscription;

        subscription.Disposable = StartTransfer(metadata)
            .Select(chunk => Observable.FromAsync(() => WriteChunkAsync(metadata, destination, chunk)))
            .Concat()
            .Concat(Observable.FromAsync(() => CompleteAsync(metadata, destination, opening is not null)))
            .Subscribe(
                _ => { },
                ex =>
                {
                    _logger.LogDebug(ex, "Transfer {SessionId} failed", metadata.SessionId);
                    CleanupTransfer(key);
                    _ = RefuseAsync(metadata);
                });
    }

    /// <summary>
    /// Hands one chunk to the consumer and writes it to the destination.
    /// </summary>
    /// <param name="metadata">The transfer the chunk belongs to.</param>
    /// <param name="destination">The stream this transfer is written to.</param>
    /// <param name="chunk">The received chunk.</param>
    /// <remarks>
    /// Awaiting the consumer handler is what keeps a slow or failing consumer visible: firing the event and
    /// dropping its task swallowed both its exceptions and its ordering. Chunks reach this method strictly
    /// one at a time, which is why the write needs no lock of its own.
    /// <para>
    /// A transfer that grows past <see cref="MaxTransferBytes"/> fails here, before the chunk is written: the
    /// sender decides how much it sends, and without the bound it decided how much memory this device spends.
    /// </para>
    /// </remarks>
    /// <exception cref="InvalidDataException">The transfer grew past <see cref="MaxTransferBytes"/>.</exception>
    private async Task WriteChunkAsync(TransferMetadata metadata, Stream destination, byte[] chunk)
    {
        if (_received.AddOrUpdate(KeyOf(metadata), chunk.Length, (_, total) => total + chunk.Length) > MaxTransferBytes)
            throw new InvalidDataException($"Transfer {metadata.SessionId} is larger than this device accepts");

        var received = OnChunkReceived;
        if (received is not null)
            await received(metadata.TransferId, chunk);

        await destination.WriteAsync(chunk);
    }

    /// <summary>
    /// Publishes the assembled payload, releases the hub session and clears local state.
    /// </summary>
    /// <param name="metadata">The transfer that finished.</param>
    /// <param name="destination">The stream the payload was written to.</param>
    /// <param name="isStreamed">Whether the destination came from the consumer.</param>
    /// <remarks>
    /// Runs as the last step of the chunk pipeline so that a failure to notify the hub surfaces as a stream
    /// error instead of an unobserved task.
    /// <para>
    /// Which event is raised follows from where the destination came from, not from its type: a consumer is
    /// free to hand over a <c>MemoryStream</c> of its own and still expect the streamed contract.
    /// </para>
    /// </remarks>
    private async Task CompleteAsync(TransferMetadata metadata, Stream destination, bool isStreamed)
    {
        await destination.FlushAsync();

        if (!isStreamed && destination is MemoryStream assembled)
        {
            var data = assembled.ToArray();

            _logger.LogDebug("Transfer {SessionId} completed. Total size: {Length} bytes", metadata.SessionId, data.Length);

            var completed = TransferCompleted;
            if (completed is not null)
                await completed(metadata.TransferId, data);
        }
        else
        {
            _logger.LogDebug("Transfer {SessionId} written to its destination", metadata.SessionId);

            var stored = TransferStored;
            if (stored is not null)
                await stored(metadata.TransferId);
        }

        await _hubConnection.SendAsync(nameof(IReactiveTransferHub.ReceiverCompleted), metadata);

        CleanupTransfer(KeyOf(metadata));
    }

    /// <summary>
    /// Opens the server stream for one session.
    /// </summary>
    /// <param name="metadata">The transfer to stream.</param>
    /// <returns>The chunks the hub sends for it.</returns>
    /// <remarks>
    /// The session's cancellation source is linked with the subscriber's own token and disposed in
    /// <c>Finally</c>, so an abandoned subscription stops the server stream instead of leaking it.
    /// </remarks>
    private IObservable<byte[]> StartTransfer(TransferMetadata metadata)
    {
        var cts = new CancellationTokenSource();
        _ctsMap[KeyOf(metadata)] = cts;

        return Observable.Create<byte[]>(async (observer, ct) =>
            {
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, cts.Token);

                try
                {
                    await foreach (var chunk in _hubConnection.StreamAsync<byte[]>(nameof(IReactiveTransferHub.StreamBytes), metadata)
                                       .WithCancellation(linkedCts.Token))
                    {
                        observer.OnNext(chunk);
                    }

                    observer.OnCompleted();
                }
                catch (Exception ex)
                {
                    observer.OnError(ex);
                }
            })
            .Finally(() =>
            {
                if (!_ctsMap.TryRemove(KeyOf(metadata), out var toCancel))
                    return;

                toCancel.Cancel();
                toCancel.Dispose();
            });
    }

    /// <summary>
    /// Tells the hub this device will not take a transfer, so it lets go of it instead of announcing it again.
    /// </summary>
    /// <param name="metadata">The transfer, with the attempt its announcement carried.</param>
    /// <remarks>
    /// A refusal that cannot reach the hub because the connection is gone is no loss: the hub hands the transfer
    /// back to its offline store on disconnect and announces it on the next connect.
    /// </remarks>
    private async Task RefuseAsync(TransferMetadata metadata)
    {
        try
        {
            await _hubConnection.SendAsync(nameof(IReactiveTransferHub.ReceiverRefused), metadata);
        }
        catch (Exception failure) when (failure is InvalidOperationException or IOException or HubException or OperationCanceledException)
        {
            _logger.LogDebug(failure, "Could not refuse transfer {SessionId}", metadata.SessionId);
        }
    }

    /// <summary>
    /// Names a transfer by its sender and session together.
    /// </summary>
    /// <param name="metadata">The announced transfer.</param>
    /// <returns>The key every dictionary here is indexed by.</returns>
    /// <remarks>
    /// Session ids are each sender's own choice. Keyed by session alone, two senders that happened to pick the same
    /// one had the second transfer dropped as "already in progress" without anyone being told.
    /// </remarks>
    private static string KeyOf(TransferMetadata metadata) => $"{metadata.TransferId}\u001F{metadata.SessionId}";

    /// <summary>
    /// Releases everything one transfer holds.
    /// </summary>
    /// <param name="sessionId">The transfer to clear, as <see cref="KeyOf"/> names it.</param>
    /// <remarks>
    /// The failure path used to pass the transfer id instead of the key, which matched nothing and freed nothing.
    /// </remarks>
    private void CleanupTransfer(string sessionId)
    {
        _received.TryRemove(sessionId, out _);

        if (_subscriptions.TryRemove(sessionId, out var subscription))
            subscription.Dispose();

        if (_ctsMap.TryRemove(sessionId, out var cts))
        {
            cts.Cancel();
            cts.Dispose();
        }

        if (!_destinations.TryRemove(sessionId, out var destination))
            return;

        destination.Dispose();
        ReleaseSlot();
    }
}
