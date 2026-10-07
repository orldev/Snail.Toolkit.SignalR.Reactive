using System.Reactive.Linq;
using System.Security.Cryptography;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using Snail.Toolkit.SignalR.Reactive.Tests.Doubles;
using Snail.Toolkit.SignalR.Reactive.Transfers;

namespace Snail.Toolkit.SignalR.Reactive.Tests;

/// <summary>
/// The hub's promises, one per test: what a sender is told, what a recipient is handed, and what the node keeps.
/// </summary>
public sealed class HubTests
{
    private static byte[] Payload(int length) => RandomNumberGenerator.GetBytes(length);

    private static Task<TransferReceipt> ReceiptOf(IReactiveTransferSender sender)
    {
        var receipt = new TaskCompletionSource<TransferReceipt>(TaskCreationOptions.RunContinuationsAsynchronously);
        sender.Acknowledged += arrived =>
        {
            receipt.TrySetResult(arrived);
            return Task.CompletedTask;
        };

        return receipt.Task;
    }

    [Fact]
    public async Task Transfer_ToAnOfflineRecipientWhileNothingIsKept_IsRefusedAtOnce()
    {
        await using var relay = new Relay(new() { ["TransferCacheOptions:PendingTransferCacheDuration"] = "0" });
        var (_, sender) = await relay.SenderAsync("alice");

        var failure = await Assert.ThrowsAsync<HubException>(() => sender.SendAsync("bob", Payload(100)));

        Assert.Contains("offline", failure.Message, StringComparison.Ordinal);
    }

    /// <summary>The old hub accepted this, dropped the chunks past the cap and delivered the truncated rest as whole.</summary>
    [Fact]
    public async Task Transfer_LargerThanTheRecipientsAllowance_FailsForTheSenderAndNeverArrives()
    {
        await using var relay = new Relay(new() { ["TransferCacheOptions:PendingTransferMaxBytes"] = "10000" });
        var (_, sender) = await relay.SenderAsync("alice");
        var small = Payload(1_000);

        await Assert.ThrowsAsync<HubException>(() => sender.SendAsync("bob", Payload(20_000), chunkSize: 4096));
        await sender.SendAsync("bob", small);
        var inbox = await relay.ReceiverAsync("bob");

        Assert.Equal(small, (await inbox.NextAsync()).Payload);
        Assert.True(await inbox.IsQuietAsync(TimeSpan.FromMilliseconds(500)));
    }

    /// <summary>A first attempt that broke off half-way must not be glued in front of the retry.</summary>
    [Fact]
    public async Task Retry_UnderTheSameSession_DeliversOnlyTheRetry()
    {
        await using var relay = new Relay();
        var (_, sender) = await relay.SenderAsync("alice");
        var retry = Payload(6_000);

        await Assert.ThrowsAsync<IOException>(() => sender.SendAsync("bob",
            Observable.Return(Payload(3_000)).Concat(Observable.Throw<byte[]>(new IOException("dropped"))), sessionId: "letter-1"));
        await sender.SendAsync("bob", retry, chunkSize: 2048, sessionId: "letter-1");
        var inbox = await relay.ReceiverAsync("bob");

        Assert.Equal(retry, (await inbox.NextAsync()).Payload);
    }

    /// <summary>One letter to a group goes to each member under one session id; every member gets their own copy.</summary>
    [Fact]
    public async Task OneSession_ToTwoRecipients_ReachesEachOfThem()
    {
        await using var relay = new Relay();
        var (_, sender) = await relay.SenderAsync("alice");
        var toBob = Payload(5_000);
        var toCarol = Payload(5_000);

        await sender.SendAsync("bob", toBob, sessionId: "group-letter");
        await sender.SendAsync("carol", toCarol, sessionId: "group-letter");
        var bob = await relay.ReceiverAsync("bob");
        var carol = await relay.ReceiverAsync("carol");

        Assert.Equal(toBob, (await bob.NextAsync()).Payload);
        Assert.Equal(toCarol, (await carol.NextAsync()).Payload);
    }

    [Fact]
    public async Task TwoSenders_UsingOneSessionIdForOneOfflineRecipient_AreKeptApart()
    {
        await using var relay = new Relay();
        var (_, alice) = await relay.SenderAsync("alice");
        var (_, dave) = await relay.SenderAsync("dave");
        var fromAlice = Payload(3_000);
        var fromDave = Payload(4_000);

        await alice.SendAsync("bob", fromAlice, sessionId: "same");
        await dave.SendAsync("bob", fromDave, sessionId: "same");
        var bob = await relay.ReceiverAsync("bob");
        await bob.NextAsync();
        await bob.NextAsync();

        Assert.Equal(fromAlice, bob.Received.Single(entry => entry.Sender == "alice").Payload);
        Assert.Equal(fromDave, bob.Received.Single(entry => entry.Sender == "dave").Payload);
    }

    [Fact]
    public async Task Receipt_NamesTheRecipientWhoTookTheTransfer()
    {
        await using var relay = new Relay();
        var bob = await relay.ReceiverAsync("bob");
        var (_, sender) = await relay.SenderAsync("alice");
        var receipt = ReceiptOf(sender);

        await sender.SendAsync("bob", Payload(2_000), sessionId: "letter-7");
        await bob.NextAsync();

        Assert.Equal(new TransferReceipt("bob", "letter-7"), await receipt.WaitAsync(Relay.Patience));
    }

    [Fact]
    public async Task Receipt_ForASenderWhoWentOffline_ArrivesWhenTheyReconnect()
    {
        await using var relay = new Relay();
        var (connection, sender) = await relay.SenderAsync("alice");
        await sender.SendAsync("bob", Payload(2_000), sessionId: "letter-8");
        await connection.StopAsync();

        var bob = await relay.ReceiverAsync("bob");
        await bob.NextAsync();
        var (_, again) = await relay.SenderAsync("alice");
        var receipt = ReceiptOf(again);

        Assert.Equal("letter-8", (await receipt.WaitAsync(Relay.Patience)).SessionId);
    }

    /// <summary>
    /// Bob's device takes the transfer but goes away before confirming it. The hub keeps it and hands it over again
    /// on Bob's next connect, instead of having deleted it before delivery.
    /// </summary>
    [Fact]
    public async Task Transfer_NotConfirmedBeforeTheRecipientDropped_IsHandedOverAgain()
    {
        await using var relay = new Relay();
        var stuck = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = await relay.ReceiverAsync("bob", receiver => receiver.TransferCompleted += async (_, _) =>
        {
            handled.TrySetResult();
            await stuck.Task;
        });
        var (_, sender) = await relay.SenderAsync("alice");
        var payload = Payload(3_000);

        await sender.SendAsync("bob", payload);
        await handled.Task.WaitAsync(Relay.Patience);
        await first.Connection.StopAsync();
        var second = await relay.ReceiverAsync("bob");

        Assert.Equal(payload, (await second.NextAsync()).Payload);
        stuck.TrySetResult();
    }

    /// <summary>
    /// The recipient subscribes half a second late. The old hub kept a window of 32 chunks and lost the start of
    /// anything longer; every chunk is held now.
    /// </summary>
    [Fact]
    public async Task Recipient_ThatSubscribesLate_StillGetsTheWholeTransfer()
    {
        await using var relay = new Relay();
        var destination = new MemoryStream();
        var stored = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await relay.ReceiverAsync("bob", receiver =>
        {
            receiver.OpenDestination = async (_, _) =>
            {
                await Task.Delay(500);
                return destination;
            };
            receiver.TransferStored += _ =>
            {
                stored.TrySetResult();
                return Task.CompletedTask;
            };
        });
        var (_, sender) = await relay.SenderAsync("alice");
        var payload = Payload(100 * 1024);

        await sender.SendAsync("bob", payload, chunkSize: 1024);
        await stored.Task.WaitAsync(Relay.Patience);

        Assert.Equal(payload, destination.ToArray());
    }

    [Fact]
    public async Task Recipient_TheApplicationDoesNotKnow_IsRefused()
    {
        await using var relay = new Relay(services: services => services.AddSingleton<ITransferRecipients>(new OnlyRecipients("bob")));
        var (_, sender) = await relay.SenderAsync("alice");

        var failure = await Assert.ThrowsAsync<HubException>(() => sender.SendAsync("nobody", Payload(100)));

        Assert.Contains("Recipient refused", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Transfer_LargerThanTheRelayAccepts_FailsWhenItCompletes()
    {
        await using var relay = new Relay(new() { ["TransferLimitOptions:MaxTransferBytes"] = "10000" });
        await relay.ReceiverAsync("bob");
        var (_, sender) = await relay.SenderAsync("alice");

        var failure = await Assert.ThrowsAsync<HubException>(() => sender.SendAsync("bob", Payload(20_000), chunkSize: 4096));

        Assert.Contains("larger", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Sender_WithTooManyTransfersOpen_IsRefusedTheNextOne()
    {
        await using var relay = new Relay(new() { ["TransferLimitOptions:MaxOpenTransfersPerSender"] = "1" });
        var (connection, _) = await relay.SenderAsync("alice");

        await connection.InvokeAsync(nameof(ReactiveTransferHub.InitiateTransfer), new TransferMetadata("bob", "first"));
        var failure = await Assert.ThrowsAsync<HubException>(() =>
            connection.InvokeAsync(nameof(ReactiveTransferHub.InitiateTransfer), new TransferMetadata("bob", "second")));

        Assert.Contains("Too many", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Ids_LongerThanAllowed_AreRefused()
    {
        await using var relay = new Relay();
        var (connection, _) = await relay.SenderAsync("alice");

        await Assert.ThrowsAsync<HubException>(() =>
            connection.InvokeAsync(nameof(ReactiveTransferHub.InitiateTransfer), new TransferMetadata(new string('x', 500), "session")));
    }

    [Fact]
    public async Task Transfer_LeftIdle_IsDroppedAndCannotBeCompleted()
    {
        await using var relay = new Relay();
        var (connection, _) = await relay.SenderAsync("alice");
        var metadata = new TransferMetadata("bob", "idle");
        await connection.InvokeAsync(nameof(ReactiveTransferHub.InitiateTransfer), metadata);
        await connection.SendAsync(nameof(ReactiveTransferHub.SendChunk), Payload(100), metadata);

        relay.Clock.Advance(TimeSpan.FromMinutes(3));
        relay.Janitor.Tidy();

        await Assert.ThrowsAsync<HubException>(() => connection.InvokeAsync(nameof(ReactiveTransferHub.CompleteTransfer), metadata));
        Assert.Equal(0, relay.Services.GetRequiredService<Parcels>().HeldBytes);
    }

    /// <summary>
    /// Kept for one hour each. The first, stored 40 minutes before the second, has expired 30 minutes after it; the
    /// second has not. The old buffer expired everything one hour after the first.
    /// </summary>
    [Fact]
    public async Task KeptTransfers_ExpireEachOnItsOwnClock()
    {
        await using var relay = new Relay();
        var (_, sender) = await relay.SenderAsync("alice");
        var later = Payload(1_000);

        await sender.SendAsync("bob", Payload(1_000));
        relay.Clock.Advance(TimeSpan.FromMinutes(40));
        await sender.SendAsync("bob", later);
        relay.Clock.Advance(TimeSpan.FromMinutes(30));
        var bob = await relay.ReceiverAsync("bob");

        Assert.Equal(later, (await bob.NextAsync()).Payload);
        Assert.True(await bob.IsQuietAsync(TimeSpan.FromMilliseconds(500)));
    }

    [Fact]
    public async Task ManySenders_ToOneOfflineRecipientAtOnce_AllArrive()
    {
        await using var relay = new Relay();
        var senders = await Task.WhenAll(Enumerable.Range(0, 8).Select(index => relay.SenderAsync($"sender-{index}")));

        await Task.WhenAll(senders.Select(pair => pair.Sender.SendAsync("bob", Payload(2_000), chunkSize: 512)));
        var bob = await relay.ReceiverAsync("bob");
        for (var index = 0; index < senders.Length; index++)
            await bob.NextAsync();

        Assert.Equal(8, bob.Received.Select(entry => entry.Sender).Distinct().Count());
    }

    /// <summary>
    /// Who wrote to whom is the one thing the relay must not write down, at any log level — SignalR's own trace of
    /// every hub call included.
    /// </summary>
    [Fact]
    public async Task Logs_NameNobody()
    {
        await using var relay = new Relay();
        var bob = await relay.ReceiverAsync("bob-the-recipient");
        var (_, sender) = await relay.SenderAsync("alice-the-sender");

        await sender.SendAsync("bob-the-recipient", Payload(2_000), sessionId: "secret-session");
        await bob.NextAsync();
        await sender.SendAsync("carol-offline", Payload(100));

        Assert.DoesNotContain(relay.Logs.Lines, line => line.Contains("alice-the-sender", StringComparison.Ordinal)
            || line.Contains("bob-the-recipient", StringComparison.Ordinal)
            || line.Contains("carol-offline", StringComparison.Ordinal)
            || line.Contains("secret-session", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Transfer_LargerThanTheDeviceAccepts_IsRefusedAndTheNextOneArrives()
    {
        await using var relay = new Relay();
        var bob = await relay.ReceiverAsync("bob", receiver => receiver.MaxTransferBytes = 4_000);
        var (_, sender) = await relay.SenderAsync("alice");
        var small = Payload(1_000);

        await Record.ExceptionAsync(() => sender.SendAsync("bob", Payload(20_000), chunkSize: 1024));
        await sender.SendAsync("bob", small);

        Assert.Equal(small, (await bob.NextAsync()).Payload);
    }

    /// <summary>The recipient refuses while the sender is still sending; the sender learns it when it completes.</summary>
    [Fact]
    public async Task Refusal_WhileTheSenderIsStillSending_FailsTheSender()
    {
        await using var relay = new Relay();
        await relay.ReceiverAsync("bob", receiver => receiver.SetChannel("calls"));
        var (_, sender) = await relay.SenderAsync("alice");
        var slow = Observable.Return(Payload(100)).Concat(Observable.Timer(TimeSpan.FromMilliseconds(500)).Select(_ => Payload(100)));

        var failure = await Assert.ThrowsAsync<HubException>(() => sender.SendAsync("bob", slow, options: new TransferOptions(Channel: "letters")));

        Assert.Contains("refused", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task User_OnTwoConnections_GetsTransfersOnTheNewestAndOnTheOtherOnceItCloses()
    {
        await using var relay = new Relay();
        var older = await relay.ReceiverAsync("bob");
        var newer = await relay.ReceiverAsync("bob");
        var (_, sender) = await relay.SenderAsync("alice");

        await sender.SendAsync("bob", Payload(500));
        await newer.NextAsync();
        await newer.Connection.StopAsync();
        await sender.SendAsync("bob", Payload(500));

        await older.NextAsync();
        Assert.Single(older.Received);
    }

    /// <summary>
    /// Bob goes away while Alice is still sending. The old hub kept a live transfer only in its stream and lost it
    /// with Bob's connection; now Alice's completion keeps it, and Bob gets all of it when he is back.
    /// </summary>
    [Fact]
    public async Task Recipient_ThatDropsWhileTheSenderIsStillSending_GetsItAllOnReconnect()
    {
        await using var relay = new Relay();
        var first = await relay.ReceiverAsync("bob");
        var (_, sender) = await relay.SenderAsync("alice");
        var head = Payload(2_000);
        var tail = new System.Reactive.Subjects.Subject<byte[]>();
        var sending = sender.SendAsync("bob", Observable.Return(head).Concat(tail));

        await Task.Delay(300);
        await first.Connection.StopAsync();
        var rest = Payload(3_000);
        tail.OnNext(rest);
        tail.OnCompleted();
        await sending.WaitAsync(Relay.Patience);
        var second = await relay.ReceiverAsync("bob");

        byte[] whole = [.. head, .. rest];
        Assert.Equal(whole, (await second.NextAsync()).Payload);
    }

    /// <summary>
    /// Bob's device is connected but the announcement never led anywhere — it was lost, or the app dropped it. After
    /// the delivery timeout the hub announces the transfer again instead of holding it forever.
    /// </summary>
    [Fact]
    public async Task Transfer_NotConfirmedWithinTheDeliveryTimeout_IsAnnouncedAgain()
    {
        await using var relay = new Relay();
        await using var bob = relay.Connection("bob");
        var announced = new SemaphoreSlim(0);
        bob.On<TransferMetadata>(nameof(IReactiveTransferClient.PrepareForTransfer), _ => announced.Release());
        await bob.StartAsync();
        var (_, sender) = await relay.SenderAsync("alice");

        await sender.SendAsync("bob", Payload(1_000));
        Assert.True(await announced.WaitAsync(Relay.Patience));
        relay.Clock.Advance(TimeSpan.FromMinutes(6));
        relay.Janitor.Tidy();

        Assert.True(await announced.WaitAsync(Relay.Patience));
    }

    /// <summary>
    /// A call sends a burst of small letters at once. Past the device's limit they used to be refused and lost;
    /// they wait for a place now, and all of them arrive.
    /// </summary>
    [Fact]
    public async Task Burst_LargerThanTheDevicesConcurrency_AllArrive()
    {
        await using var relay = new Relay();
        var bob = await relay.ReceiverAsync("bob", receiver => receiver.MaxConcurrentTransfers = 4);
        var (_, sender) = await relay.SenderAsync("alice");

        await Task.WhenAll(Enumerable.Range(0, 40).Select(index => sender.SendAsync("bob", Payload(300), sessionId: $"candidate-{index}")));
        for (var index = 0; index < 40; index++)
            await bob.NextAsync();

        Assert.Equal(40, bob.Received.Count);
    }

    private sealed class OnlyRecipients(params string[] known) : ITransferRecipients
    {
        public ValueTask<bool> AcceptsAsync(string senderId, string recipientId, CancellationToken cancellationToken) =>
            ValueTask.FromResult(known.Contains(recipientId));
    }
}
