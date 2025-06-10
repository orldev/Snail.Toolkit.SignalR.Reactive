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
    /// Sends data to a target client by automatically splitting it into chunks.
    /// </summary>
    /// <param name="targetClientId">The unique identifier of the target client.</param>
    /// <param name="bytes">The complete data to be sent.</param>
    /// <param name="chunkSize">The maximum size (in bytes) of each chunk. Default is 8192 bytes (8KB).</param>
    /// <param name="channel">The communication channel to use for the transfer. Default is "default".</param>
    /// <param name="isPending">Whether to mark the transfer as pending initially. Default is true.</param>
    /// <returns>A <see cref="Task"/> that represents the asynchronous send operation.</returns>
    /// <remarks>
    /// <para>
    /// The implementation should:
    /// <list type="bullet">
    ///   <item><description>Split the data into chunks of the specified size</description></item>
    ///   <item><description>Handle the sequential transmission of chunks</description></item>
    ///   <item><description>Generate and manage a transfer session ID</description></item>
    ///   <item><description>Ensure reliable delivery or proper error reporting</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// This method is suitable for finite data that can be fully buffered in memory.
    /// </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="targetClientId"/> or <paramref name="bytes"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="chunkSize"/> is less than or equal to 0.</exception>
    Task SendAsync(string targetClientId, byte[] bytes, int chunkSize = 8192, string channel = "default", bool isPending = true);

    /// <summary>
    /// Sends data to a target client using a reactive stream.
    /// </summary>
    /// <param name="targetClientId">The unique identifier of the target client.</param>
    /// <param name="dataStream">An observable sequence of byte arrays representing the data stream.</param>
    /// <param name="channel">The communication channel to use for the transfer. Default is "default".</param>
    /// <param name="isPending">Whether to mark the transfer as pending initially. Default is true.</param>
    /// <returns>A <see cref="Task"/> that represents the asynchronous send operation.</returns>
    /// <remarks>
    /// <para>
    /// The implementation should:
    /// <list type="bullet">
    ///   <item><description>Subscribe to the observable stream</description></item>
    ///   <item><description>Handle each emitted byte array as a data chunk</description></item>
    ///   <item><description>Generate and manage a transfer session ID</description></item>
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
    Task SendAsync(string targetClientId, IObservable<byte[]> dataStream, string channel = "default", bool isPending = true);

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