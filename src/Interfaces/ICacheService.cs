using System.Diagnostics.CodeAnalysis;

namespace Toolkit.SignalR.Reactive.Interfaces;

/// <summary>
/// Defines a generic caching service for storing and retrieving items with expiration support.
/// </summary>
public interface ICacheService
{
    /// <summary>
    /// Attempts to add an item to the cache with a specified absolute expiration time.
    /// </summary>
    /// <typeparam name="T">The type of the item to be added to the cache.</typeparam>
    /// <param name="key">The cache key identifying the entry.</param>
    /// <param name="value">The item to add to the cache.</param>
    /// <param name="absoluteExpiration">The absolute expiration time span from now.</param>
    /// <returns>
    /// <c>true</c> if the item was added successfully; <c>false</c> if the key already exists.
    /// </returns>
    /// <remarks>
    /// The cache entry will be automatically removed after the specified expiration time.
    /// </remarks>
    bool TryAdd<T>(object key, T value, TimeSpan absoluteExpiration);

    /// <summary>
    /// Attempts to get a cached item by its key.
    /// </summary>
    /// <typeparam name="T">The type of the item to retrieve.</typeparam>
    /// <param name="key">The cache key identifying the entry.</param>
    /// <param name="value">When this method returns, contains the cached item if found; otherwise, the default value.</param>
    /// <returns>
    /// <c>true</c> if the key was found in the cache; otherwise, <c>false</c>.
    /// </returns>
    bool TryGetValue<T>(object key, [MaybeNullWhen(false)] out T? value);

    /// <summary>
    /// Adds a new item or updates an existing one in the cache with a specified absolute expiration time.
    /// </summary>
    /// <typeparam name="T">The type of the item to be added or updated.</typeparam>
    /// <param name="key">The cache key identifying the entry.</param>
    /// <param name="addValueFactory">The factory method to create a new value if the key doesn't exist.</param>
    /// <param name="updateValueFactory">The factory method to update an existing value if the key exists.</param>
    /// <param name="absoluteExpiration">The absolute expiration time span from now.</param>
    /// <returns>
    /// The added or updated value from the cache.
    /// </returns>
    /// <remarks>
    /// This operation is performed atomically. The cache entry will be automatically removed after the specified expiration time.
    /// </remarks>
    T AddOrUpdate<T>(
        object key,
        Func<object, T> addValueFactory,
        Func<object, T, T> updateValueFactory,
        TimeSpan absoluteExpiration);

    /// <summary>
    /// Attempts to update an existing cached item if the current value matches the expected value.
    /// </summary>
    /// <typeparam name="T">The type of the item to update.</typeparam>
    /// <param name="key">The cache key identifying the entry.</param>
    /// <param name="oldValue">The expected current value of the cached item.</param>
    /// <param name="newValue">The new value to set if the current value matches.</param>
    /// <param name="absoluteExpiration">The absolute expiration time span from now.</param>
    /// <returns>
    /// <c>true</c> if the item was found and updated; otherwise, <c>false</c>.
    /// </returns>
    /// <remarks>
    /// This operation is performed atomically. The cache entry will be automatically removed after the specified expiration time.
    /// </remarks>
    bool TryUpdate<T>(object key, T oldValue, T newValue, TimeSpan absoluteExpiration);
    
    
    /// <summary>
    /// Attempts to remove and return a cached item by its key.
    /// </summary>
    /// <typeparam name="T">The type of the item to remove.</typeparam>
    /// <param name="key">The cache key identifying the entry.</param>
    /// <param name="value">When this method returns, contains the removed item if found; otherwise, the default value.</param>
    /// <returns>
    /// <c>true</c> if the item was found and removed; otherwise, <c>false</c>.
    /// </returns>
    bool TryRemove<T>(object key, [MaybeNullWhen(false)] out T value);

    /// <summary>
    /// Removes a cached item by its key without returning the value.
    /// </summary>
    /// <param name="key">The cache key identifying the entry to remove.</param>
    /// <remarks>
    /// This method is equivalent to <see cref="TryRemove{T}(object, out T)"/> when the removed value is not needed.
    /// </remarks>
    void Remove(object key);
}