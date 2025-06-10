namespace Toolkit.SignalR.Reactive.Entities;

/// <summary>
/// Represents the configuration options for memory caching.
/// </summary>
public class MemoryCacheOptions
{
    /// <summary>
    /// Gets or sets the duration in seconds for which user identifiers should be cached.
    /// </summary>
    /// <value>
    /// The number of seconds to cache user identifiers. The default is 0 (no caching).
    /// </value>
    public int UserIdentifierCacheDuration { get; set; }
    
    /// <summary>
    /// Gets or sets the duration in seconds for which unrecognized transfers should be cached.
    /// </summary>
    /// <value>
    /// The number of seconds to cache pending transfers. The default is 0 (no caching).
    /// </value>
    public int PendingTransferCacheDuration { get; set; }
}