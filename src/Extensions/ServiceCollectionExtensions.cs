using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Toolkit.SignalR.Reactive.Entities;
using Toolkit.SignalR.Reactive.Services;

namespace Toolkit.SignalR.Reactive.Extensions;

/// <summary>
/// Provides extension methods for <see cref="IServiceCollection"/> to configure reactive transfer services and caching.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Configures the reactive pipeline services for client-side transfer operations.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection"/> to add the services to.</param>
    /// <remarks>
    /// Registers the following services as singletons:
    /// <list type="bullet">
    /// <item><description><see cref="IReactiveTransferReceiver"/> implemented by <see cref="ReactiveTransferReceiver"/></description></item>
    /// <item><description><see cref="IReactiveTransferSender"/> implemented by <see cref="ReactiveTransferSender"/></description></item>
    /// </list>
    /// </remarks>
    public static void AddReactivePipeline(this IServiceCollection services)
    {
        services.AddSingleton<IReactiveTransferReceiver, ReactiveTransferReceiver>();
        services.AddSingleton<IReactiveTransferSender, ReactiveTransferSender>();
    }
    
    /// <summary>
    /// Configures the caching service with specified options for server-side operations.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection"/> to add the services to.</param>
    /// <param name="options">An optional action to configure the <see cref="MemoryCacheOptions"/>.</param>
    /// <remarks>
    /// <para>
    /// If no options are provided, default values will be used (no caching).
    /// </para>
    /// <para>
    /// Registers the following services:
    /// <list type="bullet">
    /// <item><description><see cref="ICacheService"/> implemented by <see cref="CacheService"/></description></item>
    /// </list>
    /// </para>
    /// </remarks>
    public static void AddCacheService(this IServiceCollection services, 
        Action<MemoryCacheOptions>? options = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.Configure(options ??= o =>
        {
            o.PendingTransferCacheDuration = 0;
            o.UserIdentifierCacheDuration = 0;
        });
        services.AddCacheService();
    }
    
    /// <summary>
    /// Configures the caching service using configuration values from the provided <see cref="IConfiguration"/>.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection"/> to add the services to.</param>
    /// <param name="configuration">The configuration containing memory cache options.</param>
    /// <remarks>
    /// <para>
    /// Expects configuration values to be in a "MemoryCacheOptions" section.
    /// </para>
    /// <para>
    /// Registers the same services as <see cref="AddCacheService(IServiceCollection, Action{MemoryCacheOptions}?)"/>.
    /// </para>
    /// </remarks>
    public static void AddCacheService(this IServiceCollection services, 
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.Configure<MemoryCacheOptions>(configuration.GetSection("MemoryCacheOptions"));
        services.AddCacheService();
    }
    
    /// <summary>
    /// Private helper method to register the core caching services.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection"/> to add the services to.</param>
    private static void AddCacheService(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddMemoryCache();
        services.AddSingleton<ICacheService, CacheService>();
    }
}