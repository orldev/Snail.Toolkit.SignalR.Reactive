using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Snail.Toolkit.SignalR.Reactive.Transfers;

/// <summary>
/// Clears what senders and recipients left behind: idle transfers, unconfirmed deliveries, expired kept transfers.
/// </summary>
/// <param name="parcels">The transfers this node holds.</param>
/// <param name="delivery">The delivery that hands transfers back to the offline store.</param>
/// <param name="deferred">The transfers kept for offline parties.</param>
/// <param name="connections">The connections each user is reachable on.</param>
/// <param name="backlog">The users whose waiting transfers are to be announced again.</param>
/// <param name="limits">How long a transfer may idle and how long a delivery may wait.</param>
/// <param name="clock">The clock the timeouts are measured by.</param>
/// <param name="logger">The logger for the summary of refusals.</param>
/// <remarks>
/// Refusals are reported as one count per round rather than one line each, so a client flooding the hub with
/// transfers it cannot have also cannot flood the log.
/// </remarks>
public sealed class TransferJanitor(
    Parcels parcels,
    TransferDelivery delivery,
    IDeferredTransfers deferred,
    IConnections connections,
    TransferBacklog backlog,
    IOptions<TransferLimitOptions> limits,
    TimeProvider clock,
    ILogger<TransferJanitor> logger) : BackgroundService
{
    /// <summary>
    /// How often the janitor makes its round.
    /// </summary>
    public static readonly TimeSpan Every = TimeSpan.FromSeconds(15);

    private readonly TransferLimitOptions _limits = limits.Value;

    /// <summary>
    /// Makes one round: drops idle transfers, hands back unconfirmed ones, expires kept ones.
    /// </summary>
    /// <remarks>
    /// A delivery handed back is queued for its recipient again when they are still connected: they are reachable,
    /// only the confirmation did not come.
    /// </remarks>
    public void Tidy()
    {
        var now = clock.GetUtcNow();

        foreach (var parcel in parcels.All())
        {
            if (!parcel.IsSealed && now - parcel.LastActivity >= _limits.IdleTimeout)
            {
                parcel.Refuse("The transfer went idle");
                parcels.Remove(parcel);
                parcels.CountRefusal();
                continue;
            }

            if (parcel.AnnouncedAt is { } announced && now - announced >= _limits.DeliveryTimeout)
            {
                delivery.Return(parcel);
                if (connections.Find(parcel.Key.Recipient) is not null)
                    backlog.Enqueue(parcel.Key.Recipient);
            }
        }

        deferred.Sweep();

        var refused = parcels.TakeRefusals();
        if (refused > 0)
            logger.LogInformation("Refused {Count} transfers since the last round", refused);
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(Every, clock);

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
                Tidy();
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }
}
