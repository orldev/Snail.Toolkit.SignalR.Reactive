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


    T AddOrUpdate<T>(
        object key,
        Func<object, T> addValueFactory,
        Func<object, T, T> updateValueFactory,
        TimeSpan absoluteExpiration);

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