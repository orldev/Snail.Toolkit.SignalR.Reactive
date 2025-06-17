namespace Toolkit.SignalR.Reactive.Entities;

/// <summary>
/// Represents metadata associated with a transfer operation.
/// </summary>
/// <param name="TransferId">The unique identifier for the transfer.</param>
/// <param name="SessionId">The session identifier associated with the transfer.</param>
/// <param name="BufferSize">The size of the buffer used for the transfer. Defaults to 1.</param>
/// <param name="Channel">The channel through which the transfer occurs. Defaults to "default".</param>
/// <param name="IsAck">Indicates whether the transfer is pending completion. Defaults to true.</param>
public record TransferMetadata(
    string TransferId, 
    string SessionId,
    int BufferSize = 1,
    string Channel = "default", 
    bool IsAck = true);