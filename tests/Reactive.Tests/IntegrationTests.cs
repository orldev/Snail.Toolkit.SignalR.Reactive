using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Security.Claims;
using System.Security.Cryptography;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Snail.Toolkit.SignalR.Reactive.Transfers;
using Snail.Toolkit.Authentication.JwtBearer;

namespace Snail.Toolkit.SignalR.Reactive.Tests;

/// <summary>
/// Drives the hub over a real SignalR connection so that routing, ownership and cleanup are asserted the
/// way a consumer experiences them rather than through the hub's internals.
/// </summary>
public class IntegrationTests : IAsyncLifetime
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);

    private readonly IHost _host;
    private readonly TestServer _server;

    public IntegrationTests()
    {
        _host = new HostBuilder()
            .ConfigureWebHost(webHost =>
            {
                webHost.UseTestServer();
                webHost.UseStartup<TestStartup>();
            })
            .Start();

        _server = _host.GetTestServer();
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    [Fact]
    public async Task HubConnection_WithValidToken_Connects()
    {
        await using var connection = CreateConnection("connects");
        await connection.StartAsync();

        Assert.Equal(HubConnectionState.Connected, connection.State);
    }

    [Fact]
    public async Task Transfer_BetweenConnectedClients_DeliversThePayload()
    {
        var payload = RandomNumberGenerator.GetBytes(20_000);

        await using var receiverConnection = CreateConnection("delivers-receiver");
        await using var receiver = new ReactiveTransferReceiver(receiverConnection,
            NullLogger<ReactiveTransferReceiver>.Instance);

        var received = Expect(receiver);
        await receiverConnection.StartAsync();

        await using var senderConnection = CreateConnection("delivers-sender");
        await senderConnection.StartAsync();
        await using var sender = new ReactiveTransferSender(senderConnection,
            NullLogger<ReactiveTransferSender>.Instance);

        await sender.SendAsync("delivers-receiver", payload, chunkSize: 4096);

        Assert.Equal(payload, await received.Task.WaitAsync(Timeout));
    }

    [Fact]
    public async Task Transfer_WhileAnotherSessionIsOpen_IsStillBuffered()
    {
        var payload = RandomNumberGenerator.GetBytes(8_000);

        await using var openConnection = CreateConnection("buffered-open");
        await using var openReceiver = new ReactiveTransferReceiver(openConnection,
            NullLogger<ReactiveTransferReceiver>.Instance);

        await openConnection.StartAsync();

        await using var senderConnection = CreateConnection("buffered-sender");
        await senderConnection.StartAsync();
        await using var sender = new ReactiveTransferSender(senderConnection,
            NullLogger<ReactiveTransferSender>.Instance);

        var openSessionLive = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        openReceiver.OnChunkReceived += (_, _) =>
        {
            openSessionLive.TrySetResult();

            return Task.CompletedTask;
        };

        var openStream = new Subject<byte[]>();
        var openTransfer = sender.SendAsync("buffered-open",
            Observable.Return(RandomNumberGenerator.GetBytes(64)).Concat(openStream));

        await openSessionLive.Task.WaitAsync(Timeout);

        await sender.SendAsync("buffered-offline", payload, chunkSize: 2048);

        await using var offlineConnection = CreateConnection("buffered-offline");
        await using var offlineReceiver = new ReactiveTransferReceiver(offlineConnection,
            NullLogger<ReactiveTransferReceiver>.Instance);

        var received = Expect(offlineReceiver);
        await offlineConnection.StartAsync();

        Assert.Equal(payload, await received.Task.WaitAsync(Timeout));

        openStream.OnCompleted();
        await openTransfer.WaitAsync(Timeout);
    }

    [Fact]
    public async Task Transfer_WithADestinationStream_IsWrittenStraightToIt()
    {
        var payload = RandomNumberGenerator.GetBytes(20_000);
        var destination = new MemoryStream();

        await using var receiverConnection = CreateConnection("streams-receiver");
        await using var receiver = new ReactiveTransferReceiver(receiverConnection,
            NullLogger<ReactiveTransferReceiver>.Instance);

        var stored = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        receiver.OpenDestination = (_, _) => Task.FromResult<Stream>(destination);
        receiver.TransferStored += _ =>
        {
            stored.TrySetResult();

            return Task.CompletedTask;
        };

        await receiverConnection.StartAsync();

        await using var senderConnection = CreateConnection("streams-sender");
        await senderConnection.StartAsync();
        await using var sender = new ReactiveTransferSender(senderConnection,
            NullLogger<ReactiveTransferSender>.Instance);

        await sender.SendAsync("streams-receiver", payload, chunkSize: 4096);
        await stored.Task.WaitAsync(Timeout);

        Assert.Equal(payload, destination.ToArray());
    }

    [Fact]
    public async Task Transfer_OnAnotherChannel_IsRefusedAndDoesNotBlockTheNextOne()
    {
        var payload = RandomNumberGenerator.GetBytes(4_000);

        await using var receiverConnection = CreateConnection("channel-receiver");
        await using var receiver = new ReactiveTransferReceiver(receiverConnection,
            NullLogger<ReactiveTransferReceiver>.Instance);

        receiver.SetChannel("expected");

        var received = Expect(receiver);
        await receiverConnection.StartAsync();

        await using var senderConnection = CreateConnection("channel-sender");
        await senderConnection.StartAsync();
        await using var sender = new ReactiveTransferSender(senderConnection,
            NullLogger<ReactiveTransferSender>.Instance);

        await Record.ExceptionAsync(() => sender.SendAsync("channel-receiver", RandomNumberGenerator.GetBytes(1_000),
            options: new TransferOptions(Channel: "unexpected")));

        await sender.SendAsync("channel-receiver", payload,
            options: new TransferOptions(Channel: "expected"));

        Assert.Equal(payload, await received.Task.WaitAsync(Timeout));
    }

    [Fact]
    public async Task StreamBytes_ForSomeoneElsesSession_IsRejected()
    {
        await using var connection = CreateConnection("intruder");
        await connection.StartAsync();

        var metadata = new TransferMetadata("victim-sender", Guid.NewGuid().ToString());

        var failure = await Assert.ThrowsAsync<HubException>(async () =>
        {
            await foreach (var _ in connection.StreamAsync<byte[]>("StreamBytes", metadata))
            {
            }
        });

        Assert.Contains("Transfer not found", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Transfer_FromAnotherProtocolVersion_IsRefused()
    {
        await using var connection = CreateConnection("old-client");
        await connection.StartAsync();

        var metadata = new TransferMetadata("anyone", Guid.NewGuid().ToString(),
            Version: TransferProtocol.Version + 1);

        var failure = await Assert.ThrowsAsync<HubException>(
            () => connection.InvokeAsync(nameof(ReactiveTransferHub.InitiateTransfer), metadata));

        Assert.Contains("Unsupported transfer protocol", failure.Message, StringComparison.Ordinal);
    }

    private static TaskCompletionSource<byte[]> Expect(IReactiveTransferReceiver receiver)
    {
        var completion = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);

        receiver.TransferCompleted += (_, data) =>
        {
            completion.TrySetResult(data);

            return Task.CompletedTask;
        };

        return completion;
    }

    private HubConnection CreateConnection(string userId)
    {
        var tokenProvider = _server.Services.GetRequiredService<ITokenProvider>();
        var accessToken = tokenProvider.Create([new Claim(ClaimTypes.NameIdentifier, userId)]);

        return new HubConnectionBuilder()
            .WithUrl(_server.BaseAddress + "reactiveTransfer", options =>
            {
                options.HttpMessageHandlerFactory = _ => _server.CreateHandler();
                options.AccessTokenProvider = () => Task.FromResult<string?>(accessToken);
            })
            .AddMessagePackProtocol()
            .Build();
    }
}
