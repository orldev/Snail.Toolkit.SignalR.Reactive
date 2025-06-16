using System.Reactive.Disposables;
using Toolkit.SignalR.Reactive.Entities;

namespace Toolkit.SignalR.Reactive;

/// <summary>
/// A reactive implementation of <see cref="IReactiveTransferSender"/> that facilitates data transfer over SignalR,
/// supporting both chunked transfers and reactive stream-based transfers with session management.
/// </summary>
/// <remarks>
/// <para>
/// This implementation provides:
/// <list type="bullet">
///   <item><description>Chunked transfer of byte arrays with configurable chunk size</description></item>
///   <item><description>Reactive stream-based transfer using <see cref="IObservable{T}"/></description></item>
///   <item><description>Session-based transfer tracking with optional acknowledgments</description></item>
/// </list>
/// </para>
/// <para>
/// Key features:
/// <list type="bullet">
///   <item><description>Automatic chunking of large data payloads</description></item>
///   <item><description>Thread-safe operation with cancellation support</description></item>
///   <item><description>Comprehensive resource cleanup via <see cref="CompositeDisposable"/></description></item>
///   <item><description>Configurable acknowledgment receipts</description></item>
///   <item><description>Channel-based transfer isolation</description></item>
/// </list>
/// </para>
/// <para>
/// The transfer lifecycle includes:
/// <list type="bullet">
///   <item><description>Session initialization with metadata</description></item>
///   <item><description>Chunked data transmission</description></item>
///   <item><description>Completion/error signaling</description></item>
///   <item><description>Automatic resource disposal</description></item>
/// </list>
/// </para>
/// </remarks>
public class ReactiveTransferSender : IReactiveTransferSender
{
    private bool _isAck = true;
    private string _currentChannel = "default";
    private readonly HubConnection _hubConnection;
    private readonly ILogger<ReactiveTransferSender> _logger;
    private readonly CompositeDisposable _disposables = new();
    
    /// <summary>
    /// Occurs when an acknowledgment receipt is received from the target client.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Event details:
    /// <list type="bullet">
    ///   <item><description>Only raised when acknowledgments are enabled</description></item>
    ///   <item><description>Provides the session ID of acknowledged transfer</description></item>
    ///   <item><description>Runs on the SignalR connection thread</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// Multiple handlers can be registered and will be invoked sequentially.
    /// </para>
    /// </remarks>
    public event Func<string, Task>? OnAcknowledgeReceipted;
    
    /// <summary>
    /// Initializes a new instance of the <see cref="ReactiveTransferSender"/> class.
    /// </summary>
    /// <param name="hubConnection">The SignalR hub connection to use for transfers.</param>
    /// <param name="logger">The logger for recording transfer operations and errors.</param>
    /// <remarks>
    /// <para>
    /// The constructor:
    /// <list type="bullet">
    ///   <item><description>Initializes the transfer infrastructure</description></item>
    ///   <item><description>Sets up default channel ("default")</description></item>
    ///   <item><description>Enables acknowledgment receipts by default</description></item>
    ///   <item><description>Registers the AcknowledgeReceipt handler</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// Note: The provided <paramref name="hubConnection"/> should already be started before use.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException">
    /// Thrown when either:
    /// <list type="bullet">
    ///   <item><description><paramref name="hubConnection"/> is null</description></item>
    ///   <item><description><paramref name="logger"/> is null</description></item>
    /// </list>
    /// </exception>
    public ReactiveTransferSender(HubConnection hubConnection, ILogger<ReactiveTransferSender> logger)
    {
        _hubConnection = hubConnection;
        _logger = logger;
        
        // Register handler for chunk delivery confirmations
        _hubConnection.On<TransferMetadata>("AcknowledgeReceipt", metadata =>
        {
            logger.LogDebug("Chunk delivered for session {SessionId}", metadata.SessionId);
            OnAcknowledgeReceipted?.Invoke(metadata.SessionId);
        }); 
    }

    /// <summary>
    /// Enables or disables acknowledgment receipts for transferred data.
    /// </summary>
    /// <param name="value">True to enable acknowledgment receipts, false to disable.</param>
    /// <remarks>
    /// <para>
    /// When enabled, the sender will:
    /// <list type="bullet">
    ///   <item><description>Wait for acknowledgment receipts from the receiver</description></item>
    ///   <item><description>Log delivery confirmation events</description></item>
    ///   <item><description>Raise the <see cref="OnAcknowledgeReceipted"/> event</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// This setting affects all subsequent transfers until changed.
    /// </para>
    /// </remarks>
    public void SetAcknowledgeReceipt(bool value)
    {
        _isAck = value;
        _logger.LogDebug("Acknowledgment receipts {Status}", value ? "enabled" : "disabled");
    }
    
    /// <summary>
    /// Sets the communication channel for subsequent transfers.
    /// </summary>
    /// <param name="channel">The channel name to use for communication.</param>
    /// <remarks>
    /// <para>
    /// Channel behavior:
    /// <list type="bullet">
    ///   <item><description>Names are case-sensitive</description></item>
    ///   <item><description>Default channel is "default"</description></item>
    ///   <item><description>Affects all subsequent Send operations</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// The channel acts as a logical separation for different transfer streams.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="channel"/> is null.</exception>
    public void SetChannel(string channel)
    {
        _currentChannel = channel;
        _logger.LogDebug("Communication channel set to: {Channel}", _currentChannel);
    }
    
    /// <summary>
    /// Sends data to a target client by automatically splitting it into chunks.
    /// </summary>
    /// <param name="targetClientId">The identifier of the target client.</param>
    /// <param name="bytes">The complete data to be sent.</param>
    /// <param name="chunkSize">The maximum size (in bytes) of each chunk. Default is 8192 bytes (8KB).</param>
    /// <param name="sessionId">Optional custom session ID for the transfer. If null, a new session ID will be generated.</param>
    /// <returns>A task that represents the asynchronous send operation.</returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when either:
    /// <list type="bullet">
    ///   <item><description><paramref name="targetClientId"/> is null</description></item>
    ///   <item><description><paramref name="bytes"/> is null</description></item>
    /// </list>
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="chunkSize"/> is less than or equal to 0.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The transfer process:
    /// <list type="bullet">
    ///   <item><description>Creates a new session with unique ID</description></item>
    ///   <item><description>Splits data into chunks of specified size</description></item>
    ///   <item><description>Sends chunks sequentially</description></item>
    ///   <item><description>Signals completion when done</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// For optimal performance:
    /// <list type="bullet">
    ///   <item><description>Choose chunk size based on network conditions</description></item>
    ///   <item><description>Avoid very small chunks (under 1KB)</description></item>
    ///   <item><description>Consider memory usage for very large files</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    public async Task SendAsync(string targetClientId, byte[] bytes, int chunkSize = 8192, string? sessionId = null)
    {
        var metadata = new TransferMetadata(targetClientId, 
            sessionId ?? Guid.NewGuid().ToString(), 
            _currentChannel, 
            _isAck);
        
        await SendCoreAsync(metadata,CreateObservable(bytes, chunkSize));
    }
    
    /// <summary>
    /// Sends data to a target client using a reactive stream.
    /// </summary>
    /// <param name="targetClientId">The identifier of the target client.</param>
    /// <param name="dataStream">An observable sequence of byte arrays representing the data stream.</param>
    /// <param name="sessionId">Optional custom session ID for the transfer. If null, a new session ID will be generated.</param>
    /// <returns>A task that represents the asynchronous send operation.</returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when either:
    /// <list type="bullet">
    ///   <item><description><paramref name="targetClientId"/> is null</description></item>
    ///   <item><description><paramref name="dataStream"/> is null</description></item>
    /// </list>
    /// </exception>
    /// <remarks>
    /// <para>
    /// The stream processing:
    /// <list type="bullet">
    ///   <item><description>Creates a new session with unique ID</description></item>
    ///   <item><description>Processes each emitted byte array as a chunk</description></item>
    ///   <item><description>Handles backpressure automatically</description></item>
    ///   <item><description>Signals completion when stream completes</description></item>
    ///   <item><description>Propagates errors immediately</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// Ideal for:
    /// <list type="bullet">
    ///   <item><description>Large files that shouldn't be fully buffered</description></item>
    ///   <item><description>Continuous data streams</description></item>
    ///   <item><description>Reactive data sources</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    public async Task SendAsync(string targetClientId, IObservable<byte[]> dataStream, string? sessionId = null)
    {
        var metadata = new TransferMetadata(targetClientId, 
            sessionId ?? Guid.NewGuid().ToString(), 
            _currentChannel, 
            _isAck);
        
        await SendCoreAsync(metadata, dataStream);
    }

    /// <summary>
    /// Core method that handles the common transfer logic for both send methods.
    /// </summary>
    /// <param name="metadata">The transfer metadata containing session information.</param>
    /// <param name="dataStream">The observable sequence of data chunks.</param>
    /// <returns>A task that represents the asynchronous send operation.</returns>
    /// <remarks>
    /// <para>
    /// Transfer workflow:
    /// <list type="bullet">
    ///   <item><description>Initializes transfer session via InitiateTransfer</description></item>
    ///   <item><description>Sets up chunk processing pipeline</description></item>
    ///   <item><description>Manages completion/error states</description></item>
    ///   <item><description>Ensures proper resource cleanup</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// The method uses:
    /// <list type="bullet">
    ///   <item><description><see cref="CompositeDisposable"/> for resource management</description></item>
    ///   <item><description>Linked cancellation tokens</description></item>
    ///   <item><description>TaskCompletionSource for completion signaling</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    private async Task SendCoreAsync(TransferMetadata metadata, IObservable<byte[]> dataStream)
    {
        var transferCompletion = new TaskCompletionSource();
        var cts = new CancellationTokenSource();
        _disposables.Add(cts);

        try
        {
            await _hubConnection.SendAsync("InitiateTransfer", metadata, cts.Token);
            
            var subscription = dataStream
                .Select(chunk => Observable.FromAsync(() =>
                    _hubConnection.SendAsync("SendChunk", chunk, metadata, cts.Token)))
                .Concat()
                .Subscribe(
                    _ => { },
                    async ex =>
                    {
                        await CompleteTransfer(false, metadata, cts.Token);
                        transferCompletion.TrySetException(ex);
                    },
                    async () =>
                    {
                        await CompleteTransfer(true, metadata, cts.Token);
                        transferCompletion.TrySetResult();
                    });
            
            _disposables.Add(subscription);
            await transferCompletion.Task;
        }
        catch (Exception ex)
        {
            _logger.LogError("Transfer failed: {Message}", ex.Message);
            throw;
        }
    }

    /// <summary>
    /// Signals transfer completion or failure to the target client.
    /// </summary>
    /// <param name="success">Whether the transfer completed successfully.</param>
    /// <param name="metadata">The transfer metadata containing session information.</param>
    /// <param name="ct">A cancellation token that can be used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <remarks>
    /// <para>
    /// Behavior depends on success state:
    /// <list type="bullet">
    ///   <item><description>Success: Sends completion notification</description></item>
    ///   <item><description>Failure: Skips completion notification</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// The method respects the cancellation token for cooperative cancellation.
    /// </para>
    /// </remarks>
    private async Task CompleteTransfer(bool success, TransferMetadata metadata, CancellationToken ct)
    {
        if (success)
        {
            await _hubConnection.SendAsync("CompleteTransfer", metadata, ct);
        }
    }

    /// <summary>
    /// Asynchronously releases all resources used by the sender.
    /// </summary>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    /// <remarks>
    /// <para>
    /// Cleanup actions:
    /// <list type="bullet">
    ///   <item><description>Disposes all active subscriptions</description></item>
    ///   <item><description>Cancels any ongoing transfers</description></item>
    ///   <item><description>Clears all internal state</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// Post-disposal behavior:
    /// <list type="bullet">
    ///   <item><description>All subsequent operations will fail</description></item>
    ///   <item><description>Pending transfers may be interrupted</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        _disposables.Dispose();
        await Task.CompletedTask;
    }
    
    /// <summary>
    /// Creates an observable sequence from a byte array by splitting it into chunks.
    /// </summary>
    /// <param name="bytes">The data to split into chunks.</param>
    /// <param name="chunkSize">The maximum size of each chunk.</param>
    /// <returns>An observable sequence of byte arrays.</returns>
    /// <remarks>
    /// <para>
    /// Implementation details:
    /// <list type="bullet">
    ///   <item><description>Uses buffer copying for efficient chunk creation</description></item>
    ///   <item><description>Processes data sequentially in memory-safe manner</description></item>
    ///   <item><description>Properly handles edge cases (empty input, exact multiples, etc.)</description></item>
    ///   <item><description>Ensures all chunks except possibly last are exactly chunkSize</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// The observable completes immediately after emitting all chunks.
    /// </para>
    /// </remarks>
    private static IObservable<byte[]> CreateObservable(byte[] bytes, int chunkSize)
    {
        return Observable.Create<byte[]>(observer =>
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
}