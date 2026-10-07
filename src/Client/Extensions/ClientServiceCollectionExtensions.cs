using Microsoft.Extensions.DependencyInjection;

namespace Snail.Toolkit.SignalR.Reactive.Extensions;

/// <summary>
/// Registers the client side of reactive transfers.
/// </summary>
/// <remarks>
/// Not named <c>ServiceCollectionExtensions</c> like its server counterpart: the two share a namespace, and one type
/// name declared in two assemblies fails to compile (CS0433) in any project that references both and names the class.
/// </remarks>
public static class ClientServiceCollectionExtensions
{
    /// <summary>
    /// Configures the reactive pipeline services for client-side transfer operations.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection"/> to add the services to.</param>
    /// <remarks>
    /// Registers the following services as singletons, over the <see cref="HubConnection"/> already registered:
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
}
