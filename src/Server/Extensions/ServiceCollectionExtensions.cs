using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Snail.Toolkit.SignalR.Reactive.Transfers;

namespace Snail.Toolkit.SignalR.Reactive.Extensions;

/// <summary>
/// Provides extension methods for <see cref="IServiceCollection"/> to configure reactive transfer services and caching.
/// </summary>
/// <remarks>
/// The client's registration, <c>AddReactivePipeline</c>, used to live here too and moved to the client package with
/// the types it registers.
/// </remarks>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the server side of transfers: the hub's state, the offline store and the workers behind it.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection"/> to add the services to.</param>
    /// <param name="options">An optional action to configure the <see cref="TransferCacheOptions"/>.</param>
    /// <remarks>
    /// Without options nothing is kept for offline recipients, and a transfer to one is refused at once. The limits
    /// in <see cref="TransferLimitOptions"/> and the log privacy in <see cref="TransferLogOptions"/> keep their
    /// defaults unless configured with <c>services.Configure</c>.
    /// </remarks>
    public static void AddReactiveTransfer(this IServiceCollection services,
        Action<TransferCacheOptions>? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.Configure(options ??= o => o.PendingTransferCacheDuration = 0);
        services.AddReactiveTransfer();
    }

    /// <summary>
    /// Adds the server side of transfers, configured from the provided <see cref="IConfiguration"/>.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection"/> to add the services to.</param>
    /// <param name="configuration">The configuration holding the sections below.</param>
    /// <remarks>
    /// Reads the "TransferCacheOptions", "TransferLimitOptions" and "TransferLogOptions" sections; a missing one
    /// keeps its defaults.
    /// </remarks>
    public static void AddReactiveTransfer(this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.Configure<TransferCacheOptions>(configuration.GetSection(nameof(TransferCacheOptions)));
        services.Configure<TransferLimitOptions>(configuration.GetSection(nameof(TransferLimitOptions)));
        services.Configure<TransferLogOptions>(configuration.GetSection(nameof(TransferLogOptions)));
        services.AddReactiveTransfer();
    }

    /// <summary>
    /// Registers the core services; anything already registered — a store, a recipient rule, a clock — is kept.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection"/> to add the services to.</param>
    private static void AddReactiveTransfer(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddMemoryCache();
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<ITransferCache, TransferCache>();
        services.TryAddSingleton<IConnections, Connections>();
        services.TryAddSingleton<IDeferredTransfers, DeferredTransfers>();
        services.TryAddSingleton<ITransferRecipients, AnyRecipient>();
        services.TryAddSingleton<TransferRedactor>();
        services.AddSingleton<Parcels>();
        services.AddSingleton<TransferDelivery>();
        services.AddSingleton<TransferBacklog>();
        services.AddHostedService<BacklogWorker>();
        services.AddHostedService<TransferJanitor>();
    }
}