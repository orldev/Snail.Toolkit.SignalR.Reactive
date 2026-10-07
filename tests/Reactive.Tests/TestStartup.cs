using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Snail.Toolkit.SignalR.Reactive.Tests.Extensions;

namespace Snail.Toolkit.SignalR.Reactive.Tests;

public class TestStartup
{
    public void ConfigureServices(IServiceCollection services)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Jwt:Issuer"] = "testIssuer",
                ["Jwt:Audience"] = "testAudience",
                ["Jwt:SecretKey"] = "ayjN7KaHE2gd2cXrG2j4wyMUP7NX8SYKZxAKm0FYo3ajNKYY3h+CQ4OYnv2WF6It",
                ["Jwt:ValidateAudience"] = "true",
                ["Jwt:ValidateIssuer"] = "true",
                ["Jwt:ValidateLifetime"] = "true",
                ["Jwt:ValidateIssuerSigningKey"] = "true",
                ["Jwt:TokenLifetime"] = "60",
                ["TransferCacheOptions:PendingTransferCacheDuration"] = "1",
            })
            .Build();
        
        services.AddReactiveTransferSignalR(configuration);
    }

    public void Configure(IApplicationBuilder app)
    {
        app.UseRouting();
        app.UseAuthentication();
        app.UseAuthorization();
        app.UseEndpoints(endpoints =>
        {
            endpoints.MapReactiveTransferHub();
        });
    }
}