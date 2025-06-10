namespace Toolkit.SignalR.Reactive.Entities;

/// <summary>
/// Represents a transfer that is currently in progress and awaiting completion.
/// </summary>
/// <param name="TransferId">The unique identifier for the transfer operation.</param>
/// <param name="Chunk">The binary data chunk associated with this transfer.</param>
/// <param name="IsComplete">Indicates whether this transfer has been marked as complete.</param>
/// <param name="Timestamp">The date and time when this transfer was created or last updated.</param>
public record PendingTransfer(
    string TransferId, 
    byte[] Chunk, 
    bool IsComplete, 
    DateTime Timestamp);