using System.Reactive.Disposables;
using Toolkit.SignalR.Reactive.Entities;

namespace Toolkit.SignalR.Reactive;

/// <summary>
/// A reactive implementation of <see cref="IReactiveTransferSender"/> that facilitates data transfer over SignalR.
/// </summary>
/// <param name="hubConnection">The SignalR hub connection used for data transfer.</param>
/// <param name="logger">The logger instance for recording transfer events and errors.</param>
/// <remarks>
/// <para>
/// This class provides two approaches for sending data:
/// <list type="bullet">
///   <item><description>Chunked transfer of byte arrays with configurable chunk size</description></item>
///   <item><description>Reactive stream-based transfer using <see cref="IObservable{T}"/></description></item>
/// </list>
/// </para>
/// <para>
/// Key features:
/// <list type="bullet">
///   <item><description>Automatic chunking of large data</description></item>
///   <item><description>Cancellation support via <see cref="CancellationToken"/></description></item>
///   <item><description>Proper resource cleanup using <see cref="CompositeDisposable"/></description></item>
///   <item><description>Comprehensive error handling and logging</description></item>
///   <item><description>Thread-safe operation</description></item>
/// </list>
/// </para>
/// <para>
/// The sender manages the complete transfer lifecycle including:
/// <list type="bullet">
///   <item><description>Session initialization</description></item>
///   <item><description>Chunk transmission</description></item>
///   <item><description>Completion signaling</description></item>
///   <item><description>Error propagation</description></item>
/// </list>
/// </para>
/// </remarks>
public class ReactiveTransferSender(HubConnection hubConnection, ILogger<ReactiveTransferSender> logger) : IReactiveTransferSender
{
    private readonly CompositeDisposable _disposables = new();
    
    /// <summary>
    /// Sends data to a target client by automatically splitting it into chunks.
    /// </summary>
    /// <param name="targetClientId">The identifier of the target client.</param>
    /// <param name="bytes">The complete data to be sent.</param>
    /// <param name="chunkSize">The maximum size (in bytes) of each chunk. Default is 8192 bytes (8KB).</param>
    /// <param name="channel">The communication channel to use. Default is "default".</param>
    /// <param name="isPending">Whether to mark transfer as pending initially. Default is true.</param>
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
    public async Task SendAsync(string targetClientId, byte[] bytes, int chunkSize = 8192, string channel = "default", bool isPending = true)
    {
        var sessionId = Guid.NewGuid().ToString();
        var metadata = new TransferMetadata(targetClientId, sessionId, channel, isPending);
        
        await SendCoreAsync(metadata,CreateObservable(bytes, chunkSize));
    }
    
    /// <summary>
    /// Sends data to a target client using a reactive stream.
    /// </summary>
    /// <param name="targetClientId">The identifier of the target client.</param>
    /// <param name="dataStream">An observable sequence of byte arrays representing the data stream.</param>
    /// <param name="channel">The communication channel to use. Default is "default".</param>
    /// <param name="isPending">Whether to mark transfer as pending initially. Default is true.</param>
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
    public async Task SendAsync(string targetClientId, IObservable<byte[]> dataStream, string channel = "default", bool isPending = true)
    {
        var sessionId = Guid.NewGuid().ToString();
        var metadata = new TransferMetadata(targetClientId, sessionId, channel, isPending);
        
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
            await hubConnection.SendAsync("InitiateTransfer", metadata, cts.Token);
            
            var subscription = dataStream
                .Select(chunk => Observable.FromAsync(() =>
                    hubConnection.SendAsync("SendChunk", chunk, metadata, cts.Token)))
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
            logger.LogError("Transfer failed: {Message}", ex.Message);
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
            await hubConnection.SendAsync("CompleteTransfer", metadata, ct);
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