global using System.Reactive.Linq;
global using System.Reactive.Subjects;
global using Microsoft.AspNetCore.SignalR;
global using Microsoft.Extensions.Logging;
global using Toolkit.SignalR.Reactive.Interfaces;
using Microsoft.Extensions.Options;
using Toolkit.SignalR.Reactive.Entities;

namespace Toolkit.SignalR.Reactive;

/// <summary>
/// A SignalR hub that facilitates reactive transfer of binary data between clients.
/// </summary>
/// <param name="logger">The logger instance for recording events and errors.</param>
/// <param name="cache">The cache service for storing pending transfers.</param>
/// <param name="options">The configuration options for memory caching.</param>
/// <remarks>
/// <para>
/// This hub manages real-time binary data transfers using reactive programming patterns.
/// It supports both connected transfers and pending transfers for offline recipients.
/// </para>
/// <para>
/// Key features:
/// <list type="bullet">
///   <item><description>Chunked binary data transfer</description></item>
///   <item><description>Transfer session management</description></item>
///   <item><description>Pending transfer handling for offline clients</description></item>
///   <item><description>Reactive stream processing using <see cref="ReplaySubject{T}"/></description></item>
/// </list>
/// </para>
/// <para>
/// The hub maintains:
/// <list type="bullet">
///   <item><description>Active transfer sessions</description></item>
///   <item><description>Client connection mappings</description></item>
///   <item><description>Pending transfers in cache</description></item>
/// </list>
/// </para>
/// </remarks>
public class ReactiveTransferHub(
    ILogger<ReactiveTransferHub> logger,
    ICacheService cache,
    IOptions<MemoryCacheOptions> options) : Hub
{
    private readonly MemoryCacheOptions _options = options.Value;
    
    /// <summary>
    /// Tracks active transfer sessions by transfer ID.
    /// </summary>
    /// <remarks>
    /// Uses a thread-safe dictionary to maintain sessions for concurrent access.
    /// Each transfer ID can have multiple active sessions.
    /// </remarks>
    private static readonly ConcurrentDictionary<string, List<TransferSession>> _activeTransfers = new();
    
    /// <summary>
    /// Maps user identifiers to their active connection IDs.
    /// </summary>
    /// <remarks>
    /// Used to quickly locate active connections for transfer operations.
    /// </remarks>
    private static readonly ConcurrentDictionary<string, string> _activeConnections = new();
    
    /// <summary>
    /// Initiates a new data transfer session with a target client.
    /// </summary>
    /// <param name="metadata">The transfer metadata containing session information.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// <para>
    /// Behavior depends on target client status:
    /// <list type="bullet">
    ///   <item><description>Connected: Prepares target client to receive data</description></item>
    ///   <item><description>Offline: Stores transfer in cache if pending transfers are enabled</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// The transfer will be associated with the current user's identifier (Context.UserIdentifier).
    /// </para>
    /// <para>
    /// If pending transfers are enabled and the target is offline, the transfer will be cached for:
    /// <see cref="MemoryCacheOptions.PendingTransferCacheDuration"/> hours.
    /// </para>
    /// </remarks>
    public async Task InitiateTransfer(TransferMetadata metadata)
    {
        var transferId = Context.UserIdentifier;
        if (string.IsNullOrEmpty(transferId)) 
            return;

        if (_activeConnections.TryGetValue(metadata.TransferId, out var connectionId) && 
            !string.IsNullOrEmpty(connectionId))
        {
            var session = CreateTransferSession(metadata);
            
            _activeTransfers.AddOrUpdate(transferId,
                _ => [session],
                (_, sessions) => 
                {
                    sessions.Add(session);
                    return sessions;
                });

            await Clients.Client(connectionId).SendAsync("PrepareForTransfer",
                metadata with {TransferId = transferId});

            logger.LogDebug("InitiateTransfer {TransferId} {TargetClientId} (Session: {SessionId})",
                transferId, metadata.TransferId, metadata.SessionId);
        }
        else if (metadata.IsAck)
        {
            if (_options.PendingTransferCacheDuration > 0)
            {
                cache.TryAdd<List<PendingTransfer>>(metadata.TransferId, [],
                    TimeSpan.FromHours(_options.PendingTransferCacheDuration));
                
                logger.LogDebug(
                    "Target client {TargetClientId} not connected, transfer {TransferId} pending",
                    metadata.TransferId, transferId);
            }
        }
    }

    /// <summary>
    /// Streams binary data chunks for a specific transfer session.
    /// </summary>
    /// <param name="metadata">The transfer metadata containing session information.</param>
    /// <returns>An async enumerable of byte arrays representing the data chunks.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when either:
    /// <list type="bullet">
    ///   <item><description>The transfer is not found</description></item>
    ///   <item><description>The session is not found for the transfer</description></item>
    /// </list>
    /// </exception>
    /// <remarks>
    /// <para>
    /// Converts the reactive <see cref="ReplaySubject{T}"/> into an <see cref="IAsyncEnumerable{T}"/> for consumption.
    /// </para>
    /// <para>
    /// The subject buffers up to 5 chunks (configured in <see cref="CreateTransferSession"/>).
    /// </para>
    /// </remarks>
    public IAsyncEnumerable<byte[]> StreamBytes(TransferMetadata metadata)
    {
        if (!_activeTransfers.TryGetValue(metadata.TransferId, out var sessions))
        {
            logger.LogWarning("Transfer not found: {TransferId}", metadata.TransferId);
            throw new InvalidOperationException("Transfer not found");
        }
        
        var session = sessions.FirstOrDefault(s => s.Metadata.SessionId == metadata.SessionId);
        if (session == null)
        {
            logger.LogWarning("Session not found: {SessionId} for transfer {TransferId}",
                metadata.SessionId, metadata.TransferId);
            throw new InvalidOperationException("Session not found");
        }

        logger.LogDebug("StreamBytes for {TransferId} (Session: {SessionId})", metadata.TransferId,
            metadata.SessionId);
        return session.Subject.AsObservable().ToAsyncEnumerable();
    }

    /// <summary>
    /// Sends a data chunk to a target client or stores it as pending.
    /// </summary>
    /// <param name="chunk">The binary data chunk to send.</param>
    /// <param name="metadata">The transfer metadata containing session information.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// <para>
    /// Routing behavior:
    /// <list type="bullet">
    ///   <item><description>Active session: Delivers chunk immediately via the subject</description></item>
    ///   <item><description>Pending transfer: Stores chunk in cache for later delivery</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// Chunks are stored with the current UTC timestamp for proper ordering during replay.
    /// </para>
    /// </remarks>
    public Task SendChunk(byte[] chunk, TransferMetadata metadata)
    {
        var transferId = Context.UserIdentifier;
        if (string.IsNullOrEmpty(transferId)) 
            return Task.CompletedTask;

        if (_activeTransfers.TryGetValue(transferId, out var sessions))
        {
            var session = sessions.FirstOrDefault(s => s.Metadata.SessionId == metadata.SessionId);
            if (session != null)
            {
                session.Subject.OnNext(chunk);
                logger.LogDebug("Sending chunk to {TransferId} (Session: {SessionId})", 
                    transferId, session.Metadata.SessionId);
            }
        }
        else if (cache.TryGetValue<List<PendingTransfer>>(metadata.TransferId, out var pending))
        {
            pending?.Add(new PendingTransfer(transferId, metadata.SessionId, chunk, metadata.BufferSize,false, DateTime.UtcNow));
            logger.LogDebug("Chunk stored as pending for {TargetClientId} (Session: {SessionId})", 
                metadata.TransferId, metadata.SessionId);
        }
        
        return Task.CompletedTask;
    }

    /// <summary>
    /// Marks a transfer as complete and cleans up resources.
    /// </summary>
    /// <param name="metadata">The transfer metadata containing session information.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// <para>
    /// For active sessions:
    /// <list type="bullet">
    ///   <item><description>Completes the subject stream</description></item>
    ///   <item><description>Logs completion event</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// For pending transfers:
    /// <list type="bullet">
    ///   <item><description>Stores completion marker in cache</description></item>
    ///   <item><description>Will be processed when target reconnects</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    public Task CompleteTransfer(TransferMetadata metadata)
    {
        var transferId = Context.UserIdentifier;
        if (string.IsNullOrEmpty(transferId)) 
            return Task.CompletedTask;
        
        if (_activeTransfers.TryGetValue(transferId, out var sessions))
        {
            var session = sessions.FirstOrDefault(s => s.Metadata.SessionId == metadata.SessionId);
            if (session != null)
            {
                session.Subject.OnCompleted();
                logger.LogDebug("Completed transfer {TransferId} (Session: {SessionId})", 
                    transferId, session.Metadata.SessionId);
            }
        }
        else if (cache.TryGetValue<List<PendingTransfer>>(metadata.TransferId, out var pending))
        {
            pending?.Add(new PendingTransfer(transferId, metadata.SessionId, [], metadata.BufferSize, true, DateTime.UtcNow));
            logger.LogDebug("Transfer completion stored as pending for {TargetClientId} (Session: {SessionId})", 
                metadata.TransferId, metadata.SessionId);
        }
        
        return Task.CompletedTask;
    }
    
    /// <summary>
    /// Notifies the hub that the receiver has completed processing the transfer.
    /// </summary>
    /// <param name="metadata">The transfer metadata containing session information.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// <para>
    /// Performs cleanup of transfer session resources including:
    /// <list type="bullet">
    ///   <item><description>Disposing the subject and subscription</description></item>
    ///   <item><description>Removing the session from active transfers</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// This should be called by the receiver after processing all chunks.
    /// </para>
    /// </remarks>
    public async Task ReceiverCompleted(TransferMetadata metadata)
    {
        if (_activeTransfers.TryGetValue(metadata.TransferId, out var sessions))
        {
            var session = sessions.FirstOrDefault(s => s.Metadata.SessionId == metadata.SessionId);
            if (session != null)
            {
                CleanupSession(metadata.TransferId, session);
                logger.LogDebug("Completed Receiver for {TransferId} (Session: {SessionId})", 
                    metadata.TransferId, metadata.SessionId);
                
                if (metadata.IsAck)
                {
                    await AcknowledgeReceipt(metadata);    
                } 
            }
        }
    }

    /// <summary>
    /// Called when a client disconnects from the hub.
    /// </summary>
    /// <param name="exception">The exception that caused the disconnection, if any.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// <para>
    /// Performs the following cleanup:
    /// <list type="bullet">
    ///   <item><description>Removes connection from active connections tracking</description></item>
    ///   <item><description>Preserves any pending transfers in cache</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        var transferId = Context.UserIdentifier;
        if (string.IsNullOrEmpty(transferId))
            return;
        
        _activeConnections.TryRemove(transferId, out _);
        
        logger.LogDebug("Disconnected {TransferId}", transferId);
        await base.OnDisconnectedAsync(exception);
    }
    
    /// <summary>
    /// Called when a client connects to the hub.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// <para>
    /// Performs the following actions:
    /// <list type="bullet">
    ///   <item><description>Tracks the new connection in active connections</description></item>
    ///   <item><description>Caches the connection information if configured</description></item>
    ///   <item><description>Processes any pending transfers for the client</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// Connection information is cached for <see cref="MemoryCacheOptions.UserIdentifierCacheDuration"/> hours.
    /// </para>
    /// </remarks>
    public override async Task OnConnectedAsync()
    {
        var transferId = Context.UserIdentifier;
        var connectionId = Context.ConnectionId;
        
        if (string.IsNullOrEmpty(transferId)) 
            return;
        
        logger.LogDebug("Connected {TransferId} {ConnectionId}", transferId, connectionId);
        
        _activeConnections.TryAdd(transferId, connectionId);
        
        // Used for search purposes
        if (_options.UserIdentifierCacheDuration > 0)
        {
            cache.TryAdd(transferId, connectionId, 
                TimeSpan.FromHours(_options.UserIdentifierCacheDuration));     
        }
        
        await ProcessPendingTransfers(transferId, connectionId);
        await ProcessPendingConfirmations(transferId);
        
        await base.OnConnectedAsync();
    }
    
    /// <summary>
    /// Creates a new transfer session with the specified parameters.
    /// </summary>
    /// <param name="metadata">The transfer metadata containing session information.</param>
    /// <returns>A new transfer session instance.</returns>
    /// <remarks>
    /// <para>
    /// The session includes:
    /// <list type="bullet">
    ///   <item><description>A replay subject with 5-chunk buffer</description></item>
    ///   <item><description>A subscription for error handling</description></item>
    ///   <item><description>The transfer metadata</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// The subject is configured to:
    /// <list type="bullet">
    ///   <item><description>Log errors</description></item>
    ///   <item><description>Clean up on error or completion</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    private TransferSession CreateTransferSession(TransferMetadata metadata)
    {
        var subject = new ReplaySubject<byte[]>(bufferSize: metadata.BufferSize);
        IDisposable? subscription = null;
        subscription = subject.Subscribe(
            onNext: _ => { },
            onError: ex => {
                logger.LogError(ex, "Transfer error for session {SessionId}", metadata.SessionId);
                CleanupSession(Context.UserIdentifier,
                    new TransferSession(subject, subscription, metadata));
            },
            onCompleted: () => { }
        );
        return new TransferSession(subject, subscription, metadata);
    }
    
    /// <summary>
    /// Sends an acknowledgment receipt for a completed transfer, with offline buffering support.
    /// </summary>
    /// <param name="metadata">The transfer metadata containing session and transfer identifiers.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    /// <remarks>
    /// <para>
    /// Behavior depends on client connectivity:
    /// <list type="bullet">
    ///   <item><description>If client is online: Immediately delivers receipt via SignalR</description></item>
    ///   <item><description>If client is offline: Buffers receipt in cache (when <see cref="_options.PendingTransferCacheDuration"/> > 0)</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// Logging includes:
    /// <list type="bullet">
    ///   <item><description>Transfer ID and Session ID for correlation</description></item>
    ///   <item><description>Buffering state for offline clients</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="metadata"/> is null.</exception>
    private async Task AcknowledgeReceipt(TransferMetadata metadata) 
    {
        if (_activeConnections.TryGetValue(metadata.TransferId, out var connectionId) &&
            !string.IsNullOrEmpty(connectionId))
        {
            await Clients.Client(connectionId).SendAsync("AcknowledgeReceipt", metadata);
        
            logger.LogDebug("Receipt acknowledged for transfer {TransferId} (Session: {SessionId})",
                metadata.TransferId, metadata.SessionId);
        }
        else
        {
            if (_options.PendingTransferCacheDuration > 0)
            {
                cache.AddOrUpdate<List<TransferMetadata>>(metadata.TransferId,
                    _ => [metadata],
                    (_, transfers) => 
                    {
                        transfers.Add(metadata);
                        return transfers;
                    }, 
                    TimeSpan.FromHours(_options.PendingTransferCacheDuration));
                
                logger.LogDebug("Buffering confirmation for offline transfer {TransferId} (Session: {SessionId})",
                    metadata.TransferId, metadata.SessionId);
            }
        }
    }
    
    /// <summary>
    /// Processes any pending transfers for a newly connected client.
    /// </summary>
    /// <param name="userId">The identifier of the user.</param>
    /// <param name="connectionId">The connection ID of the client.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// <para>
    /// Processing details:
    /// <list type="bullet">
    ///   <item><description>Transfers are grouped by transfer ID</description></item>
    ///   <item><description>Processed in chronological order</description></item>
    ///   <item><description>Completed transfers are marked after all chunks</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// After processing, pending transfers are removed from cache.
    /// </para>
    /// </remarks>
    private async Task ProcessPendingTransfers(string userId, string connectionId)
    {
        if (cache.TryGetValue<List<PendingTransfer>>(userId, out var pendingTransfers) && pendingTransfers is not null)
        {
            logger.LogDebug("Processing {PendingTransferCount} pending transfers for newly connected client {UserId}", 
                pendingTransfers.Count, userId);
            
            foreach (var transferGroup in pendingTransfers
                         .GroupBy(t => t.TransferId)
                         .OrderBy(g => g.Min(t => t.Timestamp)))
            {
                var transferId = transferGroup.Key;
                var orderedTransfers = transferGroup.OrderBy(t => t.Timestamp).ToList();
                
                var completionIndices = orderedTransfers
                    .Select((t, i) => (t.IsComplete, i))
                    .Where(x => x.IsComplete)
                    .Select(x => x.i)
                    .ToList();
                
                var startIndex = 0;
                foreach (var endIndex in completionIndices)
                {
                    var transfer = orderedTransfers.ElementAt(startIndex);
                    var metadata = new TransferMetadata(transferId, transfer.SessionId, transfer.BufferSize);
                    var segment = orderedTransfers.Skip(startIndex).Take(endIndex - startIndex);
                    await ProcessTransferSegment(metadata, connectionId, segment, true);
                    startIndex = endIndex + 1;
                }
            }
            
            cache.TryRemove<List<PendingTransfer>>(userId, out _);
        }
    }
    
    /// <summary>
    /// Processes a segment of pending transfers for a specific transfer ID.
    /// </summary>
    /// <param name="metadata">The transfer metadata containing transfer and session identifiers.</param>
    /// <param name="connectionId">The connection ID of the client.</param>
    /// <param name="transfers">The transfers to process.</param>
    /// <param name="completeAfter">Whether to mark the transfer as complete after processing.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// <para>
    /// Creates a new session and:
    /// <list type="bullet">
    ///   <item><description>Sends all chunks in order</description></item>
    ///   <item><description>Completes the stream if requested</description></item>
    ///   <item><description>Handles errors by cleaning up the session</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    private async Task ProcessTransferSegment(TransferMetadata metadata, string connectionId, 
        IEnumerable<PendingTransfer> transfers, bool completeAfter)
    {
        logger.LogDebug("Processing segment of pending transfers for transfer {TransferId} (Session: {SessionId})", 
            metadata.TransferId, metadata.SessionId);
        
        var session = CreateTransferSession(metadata);
            
        _activeTransfers.AddOrUpdate(metadata.TransferId,
            _ => [session],
            (_, sessions) => 
            {
                sessions.Add(session);
                return sessions;
            });
        
        try 
        {
            await Clients.Client(connectionId).SendAsync("PrepareForTransfer", metadata);
            
            foreach (var transfer in transfers)
            {
                session.Subject.OnNext(transfer.Chunk);
            }
        
            if (completeAfter)
            {
                session.Subject.OnCompleted();
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error processing transfer batch {TransferId} (Session: {SessionId})", 
                metadata.TransferId, metadata.SessionId);
            CleanupSession(metadata.TransferId, session);
            throw;
        }
    }
    
    /// <summary>
    /// Processes pending acknowledgment receipts for a reconnected client.
    /// </summary>
    /// <param name="userId">The identifier of the reconnected user.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    /// <remarks>
    /// <para>
    /// Workflow:
    /// <list type="bullet">
    ///   <item><description>Retrieves all buffered receipts from cache</description></item>
    ///   <item><description>Sequentially processes each receipt via <see cref="AcknowledgeReceipt"/></description></item>
    ///   <item><description>Clears processed receipts from cache</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// Logging includes:
    /// <list type="bullet">
    ///   <item><description>Count of pending confirmations</description></item>
    ///   <item><description>Completion status</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="userId"/> is null or empty.</exception>
    private async Task ProcessPendingConfirmations(string userId)
    {
        if (cache.TryGetValue<List<TransferMetadata>>(userId, out var pendingTransfers) && pendingTransfers is not null)
        {
            logger.LogDebug(
                "Processing {Count} pending confirmations for reconnected user {UserId}",
                pendingTransfers.Count, userId);

            await foreach (var transferData in pendingTransfers.ToAsyncEnumerable())
            {
                await AcknowledgeReceipt(transferData);
            }

            cache.TryRemove<List<TransferMetadata>>(userId, out _);
            
            logger.LogDebug("Completed processing pending confirmations for user {UserId}", userId);
        }
    }
    
    /// <summary>
    /// Cleans up resources for a transfer session.
    /// </summary>
    /// <param name="transferId">The identifier of the transfer.</param>
    /// <param name="session">The session to clean up.</param>
    /// <remarks>
    /// <para>
    /// Performs the following cleanup:
    /// <list type="bullet">
    ///   <item><description>Disposes the subject and subscription</description></item>
    ///   <item><description>Removes the session from active transfers</description></item>
    ///   <item><description>Cleans up empty transfer collections</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// This is called when:
    /// <list type="bullet">
    ///   <item><description>The receiver completes processing</description></item>
    ///   <item><description>An error occurs during transfer</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    private void CleanupSession(string transferId, TransferSession session)
    {
        session.Subscription.Dispose();
        session.Subject.Dispose();
        
        _activeTransfers.AddOrUpdate(transferId,
            _ => [],
            (_, sessions) => 
            {
                sessions.Remove(session);
                logger.LogDebug("Cleanup transfer session {TransferId}", transferId);
                return sessions;
            });
        
        if (_activeTransfers.TryGetValue(transferId, out var remainingSessions) && 
            remainingSessions.Count == 0)
        {
            _activeTransfers.TryRemove(transferId, out _);
            logger.LogDebug("Cleaning up active transfer {TransferId}", transferId);
        }
    }
}