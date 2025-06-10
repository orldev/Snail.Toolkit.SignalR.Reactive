global using System.Collections.Concurrent;
global using Microsoft.AspNetCore.SignalR.Client;
using Toolkit.SignalR.Reactive.Entities;

namespace Toolkit.SignalR.Reactive;

/// <summary>
/// A reactive client-side service for receiving data transfers via SignalR in chunks.
/// </summary>
/// <remarks>
/// <para>
/// This class implements <see cref="IReactiveTransferReceiver"/> to provide:
/// <list type="bullet">
///   <item><description>Chunk-by-chunk processing of incoming data</description></item>
///   <item><description>Event notifications for chunks and completed transfers</description></item>
///   <item><description>Thread-safe buffering of received data</description></item>
///   <item><description>Proper resource cleanup</description></item>
/// </list>
/// </para>
/// <para>
/// The receiver maintains separate state for each transfer session and handles
/// concurrent transfers safely using:
/// <list type="bullet">
///   <item><description><see cref="ConcurrentDictionary{TKey,TValue}"/> for session state</description></item>
///   <item><description>Synchronized memory stream access</description></item>
///   <item><description>Proper cancellation propagation</description></item>
/// </list>
/// </para>
/// </remarks>
public class ReactiveTransferReceiver : IReactiveTransferReceiver
{
    private string _currentChannel = "default";
    private readonly HubConnection _hubConnection;
    private readonly ILogger<ReactiveTransferReceiver> _logger;
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _ctsMap = new();
    private readonly ConcurrentDictionary<string, IDisposable> _subscriptions = new();
    private readonly ConcurrentDictionary<string, MemoryStream> _receivedData = new();
    
    /// <summary>
    /// Occurs when a complete transfer has been received and assembled.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The event arguments contain:
    /// <list type="bullet">
    ///   <item><description>The transfer identifier (string)</description></item>
    ///   <item><description>The complete assembled data (byte[])</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// This event is raised on the thread pool thread that processes the final chunk.
    /// </para>
    /// </remarks>
    public event Func<string, byte[], Task>? TransferCompleted;
    
    /// <summary>
    /// Occurs when a new data chunk is received for an ongoing transfer.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The event arguments contain:
    /// <list type="bullet">
    ///   <item><description>The transfer identifier (string)</description></item>
    ///   <item><description>The received data chunk (byte[])</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// This event may be raised multiple times during a single transfer session.
    /// </para>
    /// </remarks>
    public event Func<string, byte[], Task>? OnChunkReceived;
    
    /// <summary>
    /// Initializes a new instance of the <see cref="ReactiveTransferReceiver"/> class.
    /// </summary>
    /// <param name="hubConnection">The SignalR hub connection to use for transfers.</param>
    /// <param name="logger">The logger for recording events and errors.</param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when either:
    /// <list type="bullet">
    ///   <item><description><paramref name="hubConnection"/> is null</description></item>
    ///   <item><description><paramref name="logger"/> is null</description></item>
    /// </list>
    /// </exception>
    /// <remarks>
    /// <para>
    /// The constructor:
    /// <list type="bullet">
    ///   <item><description>Initializes internal state</description></item>
    ///   <item><description>Registers the "PrepareForTransfer" handler</description></item>
    ///   <item><description>Sets default channel to "default"</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    public ReactiveTransferReceiver(HubConnection hubConnection, ILogger<ReactiveTransferReceiver> logger)
    {
        _hubConnection = hubConnection;
        _logger = logger;

        // Register handler to react when the server signals readiness for transfer
        _hubConnection.On<TransferMetadata>("PrepareForTransfer", metadata =>
        {
            _logger.LogInformation("PrepareForTransfer received for ID: {TransferId}, SessionId: {SessionId}", metadata.TransferId, metadata.SessionId);
            StartAndSubscribeTransfer(metadata);
        });
    }
    
    /// <summary>
    /// Sets the current channel for receiving transfers.
    /// </summary>
    /// <param name="channel">The channel name to receive transfers from.</param>
    /// <remarks>
    /// <para>
    /// Only transfers on the specified channel will be processed after this call.
    /// </para>
    /// <para>
    /// Default channel is "default" if not specified.
    /// </para>
    /// </remarks>
    public void SetChannel(string channel)
    {
        _currentChannel = channel;
        _logger.LogDebug("Receiver now listening to channel: {Channel}", _currentChannel);
    }
    
    /// <summary>
    /// Initializes and subscribes to a new transfer session.
    /// </summary>
    /// <param name="metadata">The transfer metadata containing session information.</param>
    /// <remarks>
    /// <para>
    /// This method:
    /// <list type="bullet">
    ///   <item><description>Verifies channel compatibility</description></item>
    ///   <item><description>Creates a buffer for the incoming data (1KB initial capacity)</description></item>
    ///   <item><description>Starts the transfer stream</description></item>
    ///   <item><description>Sets up chunk processing handlers</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// If the session already exists, logs a warning and returns without action.
    /// </para>
    /// <para>
    /// The method is thread-safe and can be called concurrently for different sessions.
    /// </para>
    /// </remarks>
    private void StartAndSubscribeTransfer(TransferMetadata metadata)
    {
        if (metadata.Channel != _currentChannel)
        {
            _logger.LogDebug("Ignoring transfer - expected {CurrentChannel} but got {TransferChannel}",
                _currentChannel, metadata.Channel);
            return;
        }
        
        if (_receivedData.ContainsKey(metadata.SessionId))
        {
            _logger.LogWarning("Transfer {SessionId} already in progress.", metadata.SessionId);
            return;
        }
        
        // Initialize buffer with 1KB capacity to reduce reallocations for most cases
        var ms = new MemoryStream(capacity: 1024);
        _receivedData[metadata.SessionId] = ms;

        var observable = StartTransfer(metadata);

        var subscription = observable.Subscribe(
            chunk =>
            {
                _logger.LogDebug("Received chunk of size {Length} for transfer {SessionId}", chunk.Length, metadata.SessionId);
                
                // Invoke user-defined per-chunk processing, if any
                OnChunkReceived?.Invoke(metadata.TransferId, chunk);

                // Append chunk to buffer in thread-safe manner
                lock (ms)
                {
                    ms.Write(chunk, 0, chunk.Length);
                }
            },
            ex =>
            {
                _logger.LogError("Transfer {SessionId} failed: {Message}", metadata.SessionId, ex);
                CleanupTransfer(metadata.TransferId);
            },
            () =>
            {
                _logger.LogInformation("Transfer {SessionId} completed. Total size: {Length} bytes", metadata.SessionId, ms.Length);
                
                // Extract complete data safely
                byte[] data;
                lock (ms)
                {
                    data = ms.ToArray();
                }

                TransferCompleted?.Invoke(metadata.TransferId, data);
                _hubConnection.SendAsync("ReceiverCompleted", metadata);
                CleanupTransfer(metadata.SessionId);
            });

        if (_subscriptions.TryAdd(metadata.SessionId, subscription)) return;
        
        _logger.LogWarning("Transfer {SessionId} subscription already exists.", metadata.SessionId);
        subscription.Dispose();
    }
    
    /// <summary>
    /// Starts a new transfer stream and returns it as an observable sequence.
    /// </summary>
    /// <param name="metadata">The transfer metadata containing session information.</param>
    /// <returns>An observable sequence of byte arrays representing the data chunks.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the session is already being processed.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The observable handles:
    /// <list type="bullet">
    ///   <item><description>Streaming chunks from the server via <see cref="HubConnection.StreamAsync{T}"/></description></item>
    ///   <item><description>Cancellation propagation using linked tokens</description></item>
    ///   <item><description>Error handling and logging</description></item>
    ///   <item><description>Proper resource cleanup in Finally block</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// The stream uses the "StreamBytes" hub method to receive chunks.
    /// </para>
    /// </remarks>
    private IObservable<byte[]> StartTransfer(TransferMetadata metadata)
    {
        var cts = new CancellationTokenSource();
        if (!_ctsMap.TryAdd(metadata.SessionId, cts))
            throw new InvalidOperationException($"Transfer {metadata.SessionId} is already started.");

        var observable = Observable.Create<byte[]>(async (observer, ct) =>
        {
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, cts.Token);
            try
            {
                await foreach (var chunk in _hubConnection.StreamAsync<byte[]>("StreamBytes", metadata)
                                    .WithCancellation(linkedCts.Token))
                {
                    observer.OnNext(chunk);
                }
                observer.OnCompleted();
            }
            catch (Exception ex)
            {
                _logger.LogError("Error in transfer {TransferId}: {Message}", metadata.TransferId, ex);
                observer.OnError(ex);
            }
        }).Finally(() =>
        {
            _ctsMap.TryRemove(metadata.SessionId, out var toCancel);
            toCancel?.Cancel();
        });

        return observable;
    }
    
    /// <summary>
    /// Cleans up resources associated with a transfer session.
    /// </summary>
    /// <param name="sessionId">The unique identifier of the transfer session.</param>
    /// <remarks>
    /// <para>
    /// This method:
    /// <list type="bullet">
    ///   <item><description>Disposes the subscription if it exists</description></item>
    ///   <item><description>Cancels any ongoing operations via the CancellationTokenSource</description></item>
    ///   <item><description>Releases the memory buffer</description></item>
    ///   <item><description>Removes all session state from tracking dictionaries</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// All operations are performed in a thread-safe manner using concurrent collections.
    /// </para>
    /// </remarks>
    private void CleanupTransfer(string sessionId)
    {
        if (_subscriptions.TryRemove(sessionId, out var subscription))
        {
            subscription.Dispose();
        }
        _ctsMap.TryRemove(sessionId, out var cts);
        cts?.Cancel();

        if (_receivedData.TryRemove(sessionId, out var ms))
        {
            ms.Dispose();
        }
    }

    /// <summary>
    /// Asynchronously releases all resources used by the receiver.
    /// </summary>
    /// <returns>A <see cref="ValueTask"/> that represents the asynchronous dispose operation.</returns>
    /// <remarks>
    /// <para>
    /// This method:
    /// <list type="bullet">
    ///   <item><description>Cancels all active transfers asynchronously</description></item>
    ///   <item><description>Disposes all subscriptions</description></item>
    ///   <item><description>Clears all buffers asynchronously</description></item>
    ///   <item><description>Cleans up all internal state</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// After disposal:
    /// <list type="bullet">
    ///   <item><description>All active transfers will be canceled</description></item>
    ///   <item><description>Any attempt to use the receiver will result in undefined behavior</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// This method is thread-safe and can be called concurrently with other operations.
    /// </para>
    /// </remarks>
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

        foreach (var ms in _receivedData.Values)
        {
            await ms.DisposeAsync();
        }
        _receivedData.Clear();

        await Task.CompletedTask;
    }
}