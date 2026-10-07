using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.ResponseCompression;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Snail.Toolkit.SignalR.Reactive.Extensions;
using Snail.Toolkit.Authentication.JwtBearer;

namespace Snail.Toolkit.SignalR.Reactive.Tests.Extensions;

public static class ServiceCollectionExtensions
{
    public static void AddHubConnection(this IServiceCollection services, string baseAddress)
    {
        var hubConnection = new HubConnectionBuilder()
            .WithUrl(baseAddress + "reactiveTransfer", options =>
            {
                options.AccessTokenProvider = async () => await Task.FromResult("access_token");
            })
            .AddMessagePackProtocol()
            .ConfigureLogging(logging => logging.SetMinimumLevel(LogLevel.Information))
            .WithAutomaticReconnect()
            .Build();

        services.AddSingleton(hubConnection);
        services.AddReactivePipeline();
    }
    
    
    public static void AddReactiveTransferSignalR(
        this IServiceCollection services, 
        IConfiguration configuration,
        Action<HubOptions>? configureHub = null,
        Action<JwtBearerOptions>? configureJwt = null)
    {
        services.AddAuthJwtBearer(configuration, options =>
        {
            options.Events = new JwtBearerEvents
            {
                OnMessageReceived = context =>
                {
                    var accessToken = context.Request.Query["access_token"];
                    var path = context.HttpContext.Request.Path;
                    if (!string.IsNullOrEmpty(accessToken) && 
                        path.StartsWithSegments("/reactiveTransfer"))
                    {
                        context.Token = accessToken;
                    }
                    return Task.CompletedTask;
                }
            };
            
            configureJwt?.Invoke(options);
        });
        
        services.AddSignalR(options =>
        {
            options.MaximumReceiveMessageSize = 1024 * 92;
            options.StreamBufferCapacity = 100;
            options.EnableDetailedErrors = true;
            
            configureHub?.Invoke(options);
        }).AddMessagePackProtocol();
        
        services.AddResponseCompression(opts =>
        {
            opts.MimeTypes = ResponseCompressionDefaults.MimeTypes
                .Concat(["application/octet-stream"]);
        });
        
        services.AddReactiveTransfer(configuration);
    }
}