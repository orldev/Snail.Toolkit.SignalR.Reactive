using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;

namespace Snail.Toolkit.SignalR.Reactive.Tests.Extensions;

public static class EndpointRouteBuilderExtensions
{
    public static void MapReactiveTransferHub(this IEndpointRouteBuilder app)
    {
        ArgumentNullException.ThrowIfNull(app);
        app.MapHub<ReactiveTransferHub>("/reactiveTransfer")
            .RequireAuthorization();
    }
}