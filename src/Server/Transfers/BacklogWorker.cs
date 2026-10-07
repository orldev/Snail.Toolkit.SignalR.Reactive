using Microsoft.Extensions.Hosting;

namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// Replays buffered transfers for users as they connect.
/// </summary>
/// <param name="backlog">The users waiting to be replayed for.</param>
/// <param name="delivery">The delivery that pushes transfers and receipts.</param>
/// <param name="redactor">What the logs may say in place of ids.</param>
/// <param name="logger">The logger for replay failures.</param>
/// <remarks>
/// One user is replayed at a time so that a large backlog cannot starve the node, and a failure for one
/// user never stops the loop for the rest.
/// </remarks>
public sealed class BacklogWorker(
    TransferBacklog backlog,
    TransferDelivery delivery,
    TransferRedactor redactor,
    ILogger<BacklogWorker> logger) : BackgroundService
{
    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var userId in backlog.ReadAllAsync(stoppingToken))
        {
            try
            {
                await delivery.ReplayAsync(userId);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to replay buffered transfers for {UserId}", redactor.Name(userId));
            }
        }
    }
}
