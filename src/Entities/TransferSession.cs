namespace Toolkit.SignalR.Reactive.Entities;

/// <summary>
/// Represents an active transfer session between clients for streaming data chunks.
/// </summary>
/// <param name="Subject">The reactive subject used to publish and subscribe to data chunks during the transfer.</param>
/// <param name="Subscription">The disposable subscription that manages the lifecycle of the transfer session.</param>
/// <param name="Metadata">The metadata containing information about the transfer session.</param>
/// <remarks>
/// This record manages the real-time streaming of data chunks between clients using reactive extensions.
/// The session should be properly disposed when the transfer is complete or terminated.
/// </remarks>
public record TransferSession(
    ReplaySubject<byte[]> Subject,
    IDisposable Subscription,
    TransferMetadata Metadata);