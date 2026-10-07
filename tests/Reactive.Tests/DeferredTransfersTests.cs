using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Snail.Toolkit.SignalR.Reactive.Transfers;

namespace Snail.Toolkit.SignalR.Reactive.Tests;

public sealed class DeferredTransfersTests
{
    private readonly FakeTimeProvider _clock = new(DateTimeOffset.UtcNow);

    private DeferredTransfers Store(long perRecipient = 10_000, long total = 0, double share = 0.5) => new(Options.Create(new TransferCacheOptions
    {
        PendingTransferCacheDuration = 1,
        PendingTransferMaxBytes = perRecipient,
        PendingTransferTotalBytes = total,
        PendingTransferSenderShare = share
    }), _clock);

    private DeferredSession Session(string sender, string recipient, string session, int bytes) =>
        new(sender, recipient, session, "default", true, [new byte[bytes]], _clock.GetUtcNow());

    [Fact]
    public void Peek_LeavesTheTransferUntilItIsCommitted()
    {
        var store = Store();
        store.TryStore(Session("alice", "bob", "s", 100));

        Assert.Single(store.Peek("bob"));
        Assert.Single(store.Peek("bob"));
        store.Commit("bob", "alice", "s");
        Assert.Empty(store.Peek("bob"));
    }

    [Fact]
    public void Sender_FillingMoreThanItsShareOfARecipient_IsRefusedAndOthersStillFit()
    {
        var store = Store(perRecipient: 10_000, share: 0.5);

        Assert.True(store.TryStore(Session("alice", "bob", "a", 4_000)));
        Assert.False(store.TryStore(Session("alice", "bob", "b", 2_000)));
        Assert.True(store.TryStore(Session("dave", "bob", "c", 4_000)));
    }

    [Fact]
    public void Store_FullForEveryone_RefusesTheNextRecipientToo()
    {
        var store = Store(perRecipient: 0, total: 5_000);

        Assert.True(store.TryStore(Session("alice", "bob", "a", 4_000)));
        Assert.False(store.TryStore(Session("alice", "carol", "b", 2_000)));
    }

    [Fact]
    public void Transfer_StoredAgain_ReplacesTheEarlierOneAndFreesItsBytes()
    {
        var store = Store(perRecipient: 10_000, share: 1);

        store.TryStore(Session("alice", "bob", "s", 8_000));
        Assert.True(store.TryStore(Session("alice", "bob", "s", 8_000)));

        Assert.Single(store.Peek("bob"));
    }

    [Fact]
    public void Receipts_AreTakenOnce()
    {
        var store = Store();
        store.StoreReceipt("alice", new TransferReceipt("bob", "s"));

        Assert.Single(store.DrainReceipts("alice"));
        Assert.Empty(store.DrainReceipts("alice"));
    }
}
