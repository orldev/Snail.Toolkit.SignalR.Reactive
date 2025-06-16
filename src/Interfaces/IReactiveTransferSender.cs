namespace Toolkit.SignalR.Reactive.Interfaces;

/// <summary>
/// Defines a reactive interface for sending data transfers to target clients,
/// supporting both chunked byte array transfers and reactive stream-based transfers.
/// </summary>
/// <remarks>
/// <para>
/// This interface provides two approaches for sending data:
/// <list type="bullet">
///   <item><description>Chunked byte array transfer with configurable chunk size</description></item>
///   <item><description>Reactive stream-based transfer using <see cref="IObservable{T}"/></description></item>
/// </list>
/// </para>
/// <para>
/// Implementations should handle the underlying communication protocol, error handling,
/// and session management for reliable data transfer.
/// </para>
/// </remarks>
public interface IReactiveTransferSender
{
    /// <summary>
    /// Sets the communication channel for subsequent transfers.
    /// </summary>
    /// <param name="channel">The channel name to use for communication.</param>
    /// <remarks>
    /// <para>
    /// The channel acts as a namespace for transfers, allowing multiple independent
    /// communication streams to coexist. Channel names are case-sensitive.
    /// </para>
    /// <para>
    /// If not set explicitly, implementations should default to using "default" as the channel name.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="channel"/> is null.</exception>
    void SetChannel(string channel);
    
    /// <summary>
    /// Enables or disables acknowledgment receipts for transferred data.
    /// </summary>
    /// <param name="value">True to enable acknowledgment receipts, false to disable.</param>
    /// <remarks>
    /// <para>
    /// When enabled, the sender will expect and wait for acknowledgment receipts from
    /// the receiver for each data chunk transferred.
    /// </para>
    /// <para>
    /// This affects both chunked transfers and reactive stream transfers.
    /// </para>
    /// </remarks>
    void SetAcknowledgeReceipt(bool value);
    
    /// <summary>
    /// Occurs when an acknowledgment receipt is received from the target client.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This event is only raised when acknowledgment receipts are enabled via <see cref="SetAcknowledgeReceipt(bool)"/>.
    /// </para>
    /// <para>
    /// The string parameter contains the session ID of the acknowledged transfer.
    /// </para>
    /// </remarks>
    event Func<string, Task>? OnAcknowledgeReceipted;
    
    /// <summary>
    /// Sends data to a target client by automatically splitting it into chunks.
    /// </summary>
    /// <param name="targetClientId">The unique identifier of the target client.</param>
    /// <param name="bytes">The complete data to be sent.</param>
    /// <param name="chunkSize">The maximum size (in bytes) of each chunk. Default is 8192 bytes (8KB).</param>
    /// <param name="sessionId">Optional custom session ID for the transfer. If null, a new session ID will be generated.</param>
    /// <returns>A <see cref="Task"/> that represents the asynchronous send operation.</returns>
    /// <remarks>
    /// <para>
    /// The implementation should:
    /// <list type="bullet">
    ///   <item><description>Split the data into chunks of the specified size</description></item>
    ///   <item><description>Handle the sequential transmission of chunks</description></item>
    ///   <item><description>Use the provided or generate a new transfer session ID</description></item>
    ///   <item><description>Ensure reliable delivery or proper error reporting</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// This method is suitable for finite data that can be fully buffered in memory.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="targetClientId"/> or <paramref name="bytes"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="chunkSize"/> is less than or equal to 0.</exception>
    Task SendAsync(string targetClientId, byte[] bytes, int chunkSize = 8192, string? sessionId = null);

    /// <summary>
    /// Sends data to a target client using a reactive stream.
    /// </summary>
    /// <param name="targetClientId">The unique identifier of the target client.</param>
    /// <param name="dataStream">An observable sequence of byte arrays representing the data stream.</param>
    /// <param name="sessionId">Optional custom session ID for the transfer. If null, a new session ID will be generated.</param>
    /// <returns>A <see cref="Task"/> that represents the asynchronous send operation.</returns>
    /// <remarks>
    /// <para>
    /// The implementation should:
    /// <list type="bullet">
    ///   <item><description>Subscribe to the observable stream</description></item>
    ///   <item><description>Handle each emitted byte array as a data chunk</description></item>
    ///   <item><description>Use the provided or generate a new transfer session ID</description></item>
    ///   <item><description>Properly manage backpressure and stream completion</description></item>
    ///   <item><description>Handle stream errors gracefully</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// This method is suitable for:
    /// <list type="bullet">
    ///   <item><description>Potentially infinite data streams</description></item>
    ///   <item><description>Large data that shouldn't be fully buffered</description></item>
    ///   <item><description>Reactive data sources</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="targetClientId"/> or <paramref name="dataStream"/> is null.</exception>
    Task SendAsync(string targetClientId, IObservable<byte[]> dataStream, string? sessionId = null);

    /// <summary>
    /// Asynchronously releases all resources used by the sender.
    /// </summary>
    /// <returns>A <see cref="ValueTask"/> that represents the asynchronous dispose operation.</returns>
    /// <remarks>
    /// <para>
    /// Implementations should:
    /// <list type="bullet">
    ///   <item><description>Cancel any ongoing transfers</description></item>
    ///   <item><description>Clean up network resources</description></item>
    ///   <item><description>Release all managed and unmanaged resources</description></item>
    ///   <item><description>Complete or dispose any active observables</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// After disposal, attempting to use the sender should throw an <see cref="ObjectDisposedException"/>.
    /// </para>
    /// </remarks>
    ValueTask DisposeAsync();
}