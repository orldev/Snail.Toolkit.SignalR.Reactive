namespace Toolkit.SignalR.Reactive.Interfaces;

/// <summary>
/// Defines a reactive interface for receiving data transfers in chunks.
/// </summary>
/// <remarks>
/// This interface provides an event-driven approach to handle streaming data transfers,
/// allowing subscribers to process data chunks as they arrive and be notified when transfers complete.
/// </remarks>
public interface IReactiveTransferReceiver
{
    /// <summary>
    /// Sets the communication channel for receiving data transfers.
    /// </summary>
    /// <param name="channel">The channel name to use for receiving transfers.</param>
    void SetChannel(string channel);
    
    /// <summary>
    /// Occurs when a complete transfer has been successfully received.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The event arguments contain:
    /// <list type="bullet">
    /// <item><description>The transfer session identifier (string)</description></item>
    /// <item><description>The complete assembled data (byte[])</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// This event is raised once per transfer session after all chunks have been received and assembled.
    /// </para>
    /// </remarks>
    event Func<string, byte[], Task>? TransferCompleted;

    /// <summary>
    /// Occurs when a new data chunk is received for an ongoing transfer.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The event arguments contain:
    /// <list type="bullet">
    /// <item><description>The transfer session identifier (string)</description></item>
    /// <item><description>The received data chunk (byte[])</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// This event may be raised multiple times during a single transfer session.
    /// </para>
    /// <para>
    /// Implementations should handle chunks sequentially to ensure proper data assembly.
    /// </para>
    /// </remarks>
    event Func<string, byte[], Task>? OnChunkReceived;

    /// <summary>
    /// Asynchronously releases all resources used by the receiver.
    /// </summary>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    /// <remarks>
    /// <para>
    /// Implementations should:
    /// <list type="bullet">
    /// <item><description>Unsubscribe all event handlers</description></item>
    /// <item><description>Clean up any transfer resources</description></item>
    /// <item><description>Cancel any ongoing transfers</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// The receiver should not be reused after disposal.
    /// </para>
    /// </remarks>
    ValueTask DisposeAsync();
}