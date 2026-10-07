using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Microsoft.IdentityModel.Tokens;
using Snail.Toolkit.SignalR.Reactive.Tests.Extensions;
using Snail.Toolkit.SignalR.Reactive.Transfers;
using Snail.Toolkit.Authentication.JwtBearer;

namespace Snail.Toolkit.SignalR.Reactive.Tests.Doubles;

/// <summary>
/// A hub in a test server whose settings, clock and services a test chooses, with the clients that talk to it.
/// </summary>
/// <remarks>
/// The clock is fake, so expiry and idleness are driven by the test rather than waited for; every log line the hub
/// writes, at every level, is captured for the tests that check what the logs give away.
/// <para>
/// Tokens are checked against the same fake clock. Since Snail.Toolkit.Authentication.JwtBearer 1.1.0 a token is
/// stamped by the registered <see cref="TimeProvider"/> while the bearer handler compares it with the wall clock, so a
/// test that moved the clock an hour ahead was handed a token that was not valid yet, and its connect failed with 401.
/// </para>
/// </remarks>
public sealed class Relay : IAsyncDisposable
{
    public static readonly TimeSpan Patience = TimeSpan.FromSeconds(15);

    private readonly IHost _host;

    private readonly TestServer _server;

    private readonly List<IAsyncDisposable> _owned = [];

    public Relay(Dictionary<string, string?>? settings = null, Action<IServiceCollection>? services = null)
    {
        Dictionary<string, string?> configured = new()
        {
            ["Jwt:Issuer"] = "testIssuer",
            ["Jwt:Audience"] = "testAudience",
            ["Jwt:SecretKey"] = "ayjN7KaHE2gd2cXrG2j4wyMUP7NX8SYKZxAKm0FYo3ajNKYY3h+CQ4OYnv2WF6It",
            ["Jwt:ValidateAudience"] = "true",
            ["Jwt:ValidateIssuer"] = "true",
            ["Jwt:ValidateLifetime"] = "true",
            ["Jwt:ValidateIssuerSigningKey"] = "true",
            ["Jwt:TokenLifetime"] = "6000",
            ["TransferCacheOptions:PendingTransferCacheDuration"] = "1"
        };

        foreach (var (key, value) in settings ?? [])
            configured[key] = value;

        var configuration = new ConfigurationBuilder().AddInMemoryCollection(configured).Build();

        _host = new HostBuilder()
            .ConfigureWebHost(webHost =>
            {
                webHost.UseTestServer();
                webHost.ConfigureServices(collection =>
                {
                    collection.AddSingleton<TimeProvider>(Clock);
                    collection.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Trace).AddProvider(Logs));
                    services?.Invoke(collection);
                    collection.AddReactiveTransferSignalR(configuration,
                        configureJwt: jwt => jwt.TokenValidationParameters.LifetimeValidator = IsCurrent);
                });
                webHost.Configure(app =>
                {
                    app.UseRouting();
                    app.UseAuthentication();
                    app.UseAuthorization();
                    app.UseEndpoints(endpoints => endpoints.MapReactiveTransferHub());
                });
            })
            .Start();

        _server = _host.GetTestServer();
    }

    public FakeTimeProvider Clock { get; } = new(DateTimeOffset.UtcNow);

    public CapturedLogs Logs { get; } = new();

    public IServiceProvider Services => _host.Services;

    public TransferJanitor Janitor => Services.GetServices<IHostedService>().OfType<TransferJanitor>().Single();

    public HubConnection Connection(string userId)
    {
        var token = Services.GetRequiredService<ITokenProvider>().Create([new Claim(ClaimTypes.NameIdentifier, userId)]);

        return new HubConnectionBuilder()
            .WithUrl(_server.BaseAddress + "reactiveTransfer", options =>
            {
                options.HttpMessageHandlerFactory = _ => _server.CreateHandler();
                options.AccessTokenProvider = () => Task.FromResult<string?>(token);
            })
            .AddMessagePackProtocol()
            .Build();
    }

    public async Task<(HubConnection Connection, ReactiveTransferSender Sender)> SenderAsync(string userId)
    {
        var connection = Connection(userId);
        await connection.StartAsync();
        var sender = new ReactiveTransferSender(connection, NullLogger<ReactiveTransferSender>.Instance);

        _owned.Add(sender);
        _owned.Add(connection);
        return (connection, sender);
    }

    public async Task<Inbox> ReceiverAsync(string userId, Action<ReactiveTransferReceiver>? configure = null)
    {
        var connection = Connection(userId);
        var receiver = new ReactiveTransferReceiver(connection, NullLogger<ReactiveTransferReceiver>.Instance);
        var inbox = new Inbox(connection, receiver);

        configure?.Invoke(receiver);
        await connection.StartAsync();

        _owned.Add(receiver);
        _owned.Add(connection);
        return inbox;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var owned in Enumerable.Reverse(_owned))
            await owned.DisposeAsync();

        await _host.StopAsync();
        _host.Dispose();
    }

    private bool IsCurrent(DateTime? notBefore, DateTime? expires, SecurityToken token, TokenValidationParameters parameters)
    {
        var now = Clock.GetUtcNow().UtcDateTime;

        return (notBefore is null || notBefore <= now + parameters.ClockSkew)
            && (expires is null || expires > now - parameters.ClockSkew);
    }
}

/// <summary>
/// What one receiver has been handed, in order, with a way to wait for the next.
/// </summary>
public sealed class Inbox
{
    private readonly List<(string Sender, byte[] Payload)> _received = [];

    private readonly SemaphoreSlim _arrived = new(0);

    public Inbox(HubConnection connection, ReactiveTransferReceiver receiver)
    {
        Connection = connection;
        Receiver = receiver;
        receiver.TransferCompleted += (sender, payload) =>
        {
            lock (_received)
                _received.Add((sender, payload));

            _arrived.Release();
            return Task.CompletedTask;
        };
    }

    public HubConnection Connection { get; }

    public ReactiveTransferReceiver Receiver { get; }

    public IReadOnlyList<(string Sender, byte[] Payload)> Received
    {
        get
        {
            lock (_received)
                return [.. _received];
        }
    }

    public async Task<(string Sender, byte[] Payload)> NextAsync()
    {
        Assert.True(await _arrived.WaitAsync(Relay.Patience), "Nothing arrived in time.");

        lock (_received)
            return _received[^1];
    }

    public async Task<bool> IsQuietAsync(TimeSpan wait) => !await _arrived.WaitAsync(wait);
}

/// <summary>
/// Every log line written while a relay ran.
/// </summary>
public sealed class CapturedLogs : ILoggerProvider
{
    private readonly List<string> _lines = [];

    public IReadOnlyList<string> Lines
    {
        get
        {
            lock (_lines)
                return [.. _lines];
        }
    }

    public ILogger CreateLogger(string categoryName) => new Writer(this);

    public void Dispose()
    {
    }

    private sealed class Writer(CapturedLogs logs) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (logs._lines)
                logs._lines.Add($"{formatter(state, exception)} {exception}");
        }
    }
}
