namespace Toolkit.SignalR.Reactive.Entities;

/// <summary>
/// Represents metadata associated with a transfer operation.
/// </summary>
/// <param name="TransferId">The unique identifier for the transfer.</param>
/// <param name="SessionId">The session identifier associated with the transfer.</param>
/// <param name="Channel">The channel through which the transfer occurs. Defaults to "default".</param>
/// <param name="IsAck">Indicates whether the transfer is pending completion. Defaults to true.</param>
public record TransferMetadata(
    string TransferId, 
    string SessionId,
    string Channel = "default", 
    bool IsAck = true);