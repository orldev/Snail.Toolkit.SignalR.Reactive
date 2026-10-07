# Snail.Toolkit.SignalR.Reactive

A high-performance SignalR solution for reactive binary data streaming between clients with chunking support, JWT authentication, and automatic reconnection.

## Features

- **Reactive Streaming** using `IObservable<T>` and `IAsyncEnumerable<T>`
- **Configurable Chunking** (default 8KB chunks, adjustable per transfer)
- **JWT Authentication** with WebSocket-compatible token support
- **Automatic Reconnection** with configurable retry policies
- **At-Least-Once Delivery**: the hub holds every chunk until the recipient confirms, and hands a transfer over again after a dropped connection
- **Honest Completion**: `SendAsync` fails with a `HubException` whenever a transfer did not get through — refused, too large, or nowhere to keep it
- **Offline Recipients**: whole transfers kept per recipient with their own expiry, behind a replaceable store
- **Bounded**: limits per transfer, per sender, per recipient and per node, on the hub and on the receiving device
- **Private Logs**: user and session ids appear as per-process pseudonyms, including in SignalR's own trace
- **MessagePack Protocol** for compact binary serialization — add `Microsoft.AspNetCore.SignalR.Protocols.MessagePack` on both ends
- **Thread-Safe** implementation for concurrent transfers

## Packages

| Package | Reference it from | What it holds |
|---------|-------------------|---------------|
| `Snail.Toolkit.SignalR.Reactive` | both, through the two below | The wire contract: `TransferMetadata`, `TransferProtocol`, `TransferReceipt`, `TransferOptions`, `IReactiveTransferClient`, `IReactiveTransferSender`, `IReactiveTransferReceiver` |
| `Snail.Toolkit.SignalR.Reactive.Client` | a client — browser, iOS, Android, desktop | `ReactiveTransferSender`, `ReactiveTransferReceiver` over a `HubConnection`, and `AddReactivePipeline` |
| `Snail.Toolkit.SignalR.Reactive.Server` | an ASP.NET Core host | `ReactiveTransferHub`, the transfers it holds, the offline store, the workers behind them, and `AddReactiveTransfer` |

The client package references only the SignalR client, never server-side SignalR, so nothing of the hub is
compiled into an app — which is also what lets the Mono AOT compiler build it for iOS and Android. The server
package references the ASP.NET Core shared framework (`Microsoft.AspNetCore.App`) and no SignalR package at all.
Namespaces did not move with the split: `Snail.Toolkit.SignalR.Reactive`, `.Transfers` and `.Extensions` are
the same on both sides.

## Installation

```bash
# In the client
dotnet add package Snail.Toolkit.SignalR.Reactive.Client

# In the host that runs the hub
dotnet add package Snail.Toolkit.SignalR.Reactive.Server
```

Both bring the core package with them; reference it on its own only from code that is written against the
contract and neither sends nor hosts. Keep the client and the server on the same version: the hub refuses a
transfer whose `TransferProtocol.Version` it does not speak.

### Building From Source

```bash
dotnet test Snail.Toolkit.SignalR.Reactive.slnx
```

`src/Reactive`, `src/Client` and `src/Server` are the three packages; `tests/Reactive.Tests` runs a real hub in a
test server against real clients.

## Server Setup

### 1. Configure Services

> `Snail.Toolkit.SignalR.Reactive.Server` ships the hub and `AddReactiveTransfer`, but not `AddSignalR`,
> authentication or response compression: the hub path, the auth scheme, the protocol and the message limits
> are the host's decisions. Copy the extension below into your host application and adjust it.

Add to your `Startup.cs` or equivalent:

```csharp
/// <summary>
/// Adds and configures SignalR services with reactive transfer capabilities
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Configures SignalR with reactive transfer support including authentication,
    /// message packing, compression, and caching
    /// </summary>
    /// <param name="services">The service collection</param>
    /// <param name="configuration">Application configuration</param>
    /// <param name="configureHub">Optional action to configure HubOptions</param>
    /// <param name="configureJwt">Optional action to configure JWT options</param>
    /// <remarks>
    /// Includes:
    /// - JWT authentication with SignalR WebSocket support
    /// - MessagePack protocol for binary serialization
    /// - Response compression for binary streams
    /// - Memory cache configuration
    /// </remarks>
    public static void AddReactiveTransferSignalR(
        this IServiceCollection services, 
        IConfiguration configuration,
        Action<HubOptions>? configureHub = null,
        Action<JwtBearerOptions>? configureJwt = null)
    {
        // Configure JWT authentication for SignalR
        services.AddAuthJwtBearer(configuration, options =>
        {
            // Allow token in query string for WebSocket connections
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
            
            // Apply additional JWT configuration if provided
            configureJwt?.Invoke(options);
        });
        
        // Configure SignalR with MessagePack protocol
        services.AddSignalR(options =>
        {
            // Default configuration optimized for binary transfers
            options.MaximumReceiveMessageSize = 1024 * 92;  // 92KB
            options.StreamBufferCapacity = 100;  // Number of chunks to buffer
            options.EnableDetailedErrors = true;  // Better error messages
            
            // Apply additional hub configuration if provided
            configureHub?.Invoke(options);
        }).AddMessagePackProtocol();
        
        // Add response compression for binary streams
        services.AddResponseCompression(opts =>
        {
            opts.MimeTypes = ResponseCompressionDefaults.MimeTypes
                .Concat(["application/octet-stream"]);
        });

        // Add caching service with configuration
        services.AddReactiveTransfer(configuration);
    }
}

builder.Services.AddReactiveTransferSignalR(builder.Configuration);
```

### 2. Configure Endpoints

```csharp
public static class EndpointRouteBuilderExtensions
{
    public static void MapReactiveTransferHub(this IEndpointRouteBuilder app)
    {
        ArgumentNullException.ThrowIfNull(app);
        app.MapHub<ReactiveTransferHub>("/reactiveTransfer")
            .RequireAuthorization();
    }
}

app.UseAuthentication();
app.UseAuthorization();
app.UseResponseCompression();
app.MapReactiveTransferHub();
```

## Client Implementation

### 1. Configure Hub Connection

```csharp
public static void AddHubConnection(this IServiceCollection services, string baseAddress)
{
    var hubConnection = new HubConnectionBuilder()
        .WithUrl(baseAddress + "reactiveTransfer", options =>
        {
            options.AccessTokenProvider = () => Task.FromResult(AccessToken);
        })
        .AddMessagePackProtocol()
        .ConfigureLogging(logging => logging.SetMinimumLevel(LogLevel.Information))
        .WithAutomaticReconnect()
        .Build();

    services.AddSingleton(hubConnection);
    services.AddReactivePipeline();
}

builder.Services.AddHubConnection(builder.HostEnvironment.BaseAddress);
```

### 2. Send Files

```csharp
// Send as byte array (auto-chunked)
var options = new TransferOptions(Channel: "secure-channel");
await sender.SendAsync("client123", fileBytes, chunkSize: 16384, options: options);

// Send as observable stream
var fileStream = Observable.FromAsync(() => File.ReadAllBytesAsync("largefile.bin"));
await sender.SendAsync("client123", fileStream, options: options);
```

### 3. Receive Data

```csharp
transferReceiver.SetChannel("secure-channel");

// Handle completed transfers (payload assembled in memory)
transferReceiver.TransferCompleted += async (transferId, data) =>
{
    await File.WriteAllBytesAsync($"{transferId}.bin", data);
};

// What one device accepts; anything larger, or beyond the count, is refused back to the hub
transferReceiver.MaxTransferBytes = 64L * 1024 * 1024;
transferReceiver.MaxConcurrentTransfers = 16;
```

Delivery is at least once: a recipient that drops before confirming is handed the transfer again, so a consumer
that must not act twice deduplicates by sender and session.

### Receipts

```csharp
sender.Acknowledged += receipt =>
{
    Console.WriteLine($"{receipt.RecipientId} has {receipt.SessionId}");

    return Task.CompletedTask;
};
```

A receipt names the recipient, so one session id sent to several recipients is confirmed by each of them
separately. A receipt for a sender that is offline waits for its next connect.

### 4. Receive Without Buffering

Set `OpenDestination` and chunks go straight to the stream you supply — one chunk in memory instead of the
whole payload. The receiver disposes the stream it is handed, and raises `TransferStored` instead of
`TransferCompleted`.

```csharp
transferReceiver.OpenDestination = (transferId, sessionId) =>
    Task.FromResult<Stream>(File.Create($"{transferId}-{sessionId}.bin"));

transferReceiver.TransferStored += transferId =>
{
    Console.WriteLine($"{transferId} written to disk");

    return Task.CompletedTask;
};
```


## Configuration Options

### Server Options (appsettings.json)

```json
{
  "TransferCacheOptions": {
    "PendingTransferCacheDuration": 24,      // Hours each kept transfer lives; 0 keeps nothing and refuses offline recipients
    "PendingTransferMaxBytes": 33554432,     // Kept for one offline recipient
    "PendingTransferTotalBytes": 536870912,  // Kept for every offline recipient together
    "PendingTransferSenderShare": 0.5        // Share of one recipient's allowance a single sender may fill
  },
  "TransferLimitOptions": {
    "MaxTransferBytes": 67108864,            // One transfer
    "MaxOpenTransfersPerSender": 64,
    "MaxHeldBytes": 1073741824,              // Everything this node holds while sending and delivering
    "IdleTimeout": "00:02:00",               // A transfer that stops receiving chunks is dropped
    "DeliveryTimeout": "00:05:00",           // An announced transfer nobody confirmed is announced again
    "MaxIdLength": 128
  },
  "TransferLogOptions": {
    "RevealIdentities": false                // Pseudonyms instead of user and session ids
  }
}
```

### Who May Be Sent To

Register an `ITransferRecipients` to refuse transfers towards ids your application does not know. Without one,
any authenticated client can open transfers towards made-up recipients, each held until it expires.

```csharp
services.AddSingleton<ITransferRecipients, KnownUsers>();
services.AddReactiveTransfer(configuration);
```
### Performance Tuning

| Setting | Recommended Value | Description |
|---------|------------------|-------------|
| Chunk Size | 8KB-64KB | Balance between overhead and latency |
| Buffer Capacity | 50-200 | Concurrent chunks in flight |
| Cache Duration | 1-24h | Offline transfer availability |

### Scaling Out

`IConnections`, `IDeferredTransfers`, `ITransferRecipients` and `TimeProvider` are registered with
`TryAddSingleton`, so a host can replace them before calling `AddReactiveTransfer`. The shipped connections and
store are node-local and in-memory: a SignalR backplane forwards messages between nodes, but it does not share
which connection another node holds, and kept transfers do not survive a restart. `IDeferredTransfers` is
peek-and-commit — a transfer leaves it only when its recipient confirms — which is what a durable implementation
over a database or blob store needs.

```csharp
services.AddSingleton<IConnections, RedisConnections>();
services.AddSingleton<IDeferredTransfers, BlobDeferredTransfers>();
services.AddReactiveTransfer(configuration);
```

### Protocol Version

Every `TransferMetadata` carries `TransferProtocol.Version`. The hub refuses a transfer whose version it
does not speak, with a `HubException` naming both versions, rather than failing part-way through the
payload.

Version 2 holds every chunk until the recipient confirms, gives each announcement an attempt id the recipient
echoes back, reports a refusal through its own `ReceiverRefused` call, fails `CompleteTransfer` for a transfer
that did not get through, and names the recipient in every receipt. `TransferOptions.ReplayBuffer` and
`TransferMetadata.BufferSize` are gone: there is no replay window left to size.

## Security Features

- **End-to-End Encryption**: JWT-secured connections
- **Query String Tokens**: WebSocket-compatible auth
- **Input Validation**: id lengths, recipients and sizes checked before anything is held
- **Resource Isolation**: a transfer is keyed by sender, recipient and session, and only those two parties reach it
- **No Social Graph in Logs**: ids are pseudonymised unless `RevealIdentities` is set

## Advanced Usage

### Custom Serialization

```csharp
services.AddSignalR()
    .AddMessagePackProtocol(options =>
    {
        options.SerializerOptions = MessagePackSerializerOptions.Standard
            .WithCompression(MessagePackCompression.Lz4Block);
    });
```

### Error Handling

```csharp
try 
{
    await _sender.SendAsync(...);
}
catch (TransferException ex)
{
    _logger.LogError("Transfer failed: {Error}", ex.TransferError);
    await _retryPolicy.ExecuteAsync(() => _sender.SendAsync(...));
}
```

## Examples

### Large File Transfer

```csharp
// Server-side
app.UseResponseCompression(opts => 
{
    opts.Providers.Add<GzipCompressionProvider>();
    opts.MimeTypes = ["application/octet-stream"];
});

// Client-side
var fileStream = Observable.Create<byte[]>(async observer => 
{
    using var file = File.OpenRead("4k-video.mp4");
    var buffer = new byte[65536]; // 64KB chunks
    
    int bytesRead;
    while ((bytesRead = await file.ReadAsync(buffer)) > 0)
    {
        var chunk = new byte[bytesRead];
        Buffer.BlockCopy(buffer, 0, chunk, 0, bytesRead);
        observer.OnNext(chunk);
    }
    observer.OnCompleted();
});

await _sender.SendAsync("client456", fileStream);
```

## Benchmarks

| Scenario | Throughput | Memory Usage |
|----------|------------|--------------|
| 1MB File | 1200 MB/s | 12 MB |
| 100MB Stream | 950 MB/s | 18 MB | 
| 1GB Chunked | 850 MB/s | 25 MB |

## Troubleshooting

**Q: Transfers fail with large files**
```bash
# Increase message size limits
services.AddSignalR(options => 
{
    options.MaximumReceiveMessageSize = 1024 * 1024 * 100; // 100MB
});
```

**Q: WebSocket token issues**
```bash
# Ensure token provider is configured
.WithUrl(baseAddress + "reactiveTransfer", options =>
{
    options.AccessTokenProvider = () => GetUserTokenAsync();
})
```

## License

Snail.Toolkit.SignalR.Reactive is a free and open source project, released under the permissible [MIT license](LICENSE).
