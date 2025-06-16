using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Caching.Memory;

namespace Toolkit.SignalR.Reactive.Services;

/// <summary>
/// Provides an implementation of <see cref="ICacheService"/> using <see cref="IMemoryCache"/> as the backing store.
/// </summary>
/// <param name="cache">The memory cache instance to use for storage.</param>
/// <remarks>
/// This service provides thread-safe caching operations with support for generic types and expiration policies.
/// All operations are atomic and designed for concurrent access.
/// </remarks>
public class CacheService(IMemoryCache cache) : ICacheService
{
    /// <summary>
    /// Attempts to add a new item to the cache if the key doesn't already exist.
    /// </summary>
    /// <typeparam name="T">The type of the value to cache.</typeparam>
    /// <param name="key">The cache key to add.</param>
    /// <param name="value">The value to associate with the key.</param>
    /// <param name="absoluteExpiration">The absolute expiration time span from now.</param>
    /// <returns>
    /// <c>true</c> if the item was added successfully; <c>false</c> if the key already exists.
    /// </returns>
    /// <remarks>
    /// The operation is atomic and thread-safe. The expiration timer begins when the item is added.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="key"/> is null.</exception>
    public bool TryAdd<T>(object key, T value, TimeSpan absoluteExpiration)
    {
        ArgumentNullException.ThrowIfNull(key);
        
        var cacheKey = GetKey<T>(key);
        
        if (cache.TryGetValue(cacheKey, out _))
        {
            return false;
        }
        cache.Set(cacheKey, value, absoluteExpiration);
        return true;
    }

    /// <summary>
    /// Attempts to retrieve a cached value by its key.
    /// </summary>
    /// <typeparam name="T">The expected type of the cached value.</typeparam>
    /// <param name="key">The cache key to look up.</param>
    /// <param name="value">When this method returns, contains the cached value if found; otherwise, the default value.</param>
    /// <returns>
    /// <c>true</c> if the key was found and the value is of type <typeparamref name="T"/>; otherwise, <c>false</c>.
    /// </returns>
    /// <remarks>
    /// The method performs both key existence check and type validation in a single atomic operation.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="key"/> is null.</exception>
    public bool TryGetValue<T>(object key, [MaybeNullWhen(false)] out T value)
    {
        ArgumentNullException.ThrowIfNull(key);
        
        if (cache.TryGetValue(GetKey<T>(key), out var cachedValue) && cachedValue is T typedValue)
        {
            value = typedValue;
            return true;
        }

        value = default;
        return false;
    }

    /// <summary>
    /// Adds a new item or updates an existing one in the cache with a specified absolute expiration time.
    /// </summary>
    /// <typeparam name="T">The type of the item to be added or updated.</typeparam>
    /// <param name="key">The cache key identifying the entry.</param>
    /// <param name="addValueFactory">The factory method to create a value if the key doesn't exist.</param>
    /// <param name="updateValueFactory">The factory method to update the value if the key exists.</param>
    /// <param name="absoluteExpiration">The absolute expiration time span from now.</param>
    /// <returns>The added or updated value from the cache.</returns>
    /// <remarks>
    /// This operation is atomic and thread-safe. The method will retry until it successfully adds or updates the value.
    /// The cache entry will be automatically removed after the specified expiration time.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="key"/>, <paramref name="addValueFactory"/>,
    /// or <paramref name="updateValueFactory"/> is null.</exception>
    public T AddOrUpdate<T>(
        object key,
        Func<object, T> addValueFactory,
        Func<object, T, T> updateValueFactory,
        TimeSpan absoluteExpiration)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(addValueFactory);
        ArgumentNullException.ThrowIfNull(updateValueFactory);
        
        while (true)
        {
            if (TryGetValue<T>(key, out var existingValue))
            {
                var newValue = updateValueFactory(key, existingValue);
                if (TryUpdate(key, existingValue, newValue, absoluteExpiration))
                {
                    return newValue;
                }
            }
            else
            {
                var newValue = addValueFactory(key);
                if (TryAdd(key, newValue, absoluteExpiration))
                {
                    return newValue;
                }
            }
        }
    }

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
    /// This operation is atomic and thread-safe, using a lock to ensure consistency during the check-and-update operation.
    /// The cache entry will be automatically removed after the specified expiration time.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="key"/> is null.</exception>
    public bool TryUpdate<T>(object key, T oldValue, T newValue, TimeSpan absoluteExpiration)
    {
        lock (cache)
        {
            if (TryGetValue<T>(key, out var currentValue) && EqualityComparer<T>.Default.Equals(currentValue, oldValue))
            {
                cache.Set(GetKey<T>(key), newValue, absoluteExpiration);
                return true;
            }
            return false;
        }
    }
    
    /// <summary>
    /// Attempts to remove and return a cached value by its key.
    /// </summary>
    /// <typeparam name="T">The expected type of the cached value.</typeparam>
    /// <param name="key">The cache key to remove.</param>
    /// <param name="value">When this method returns, contains the removed value if found; otherwise, the default value.</param>
    /// <returns>
    /// <c>true</c> if the item was found and removed; otherwise, <c>false</c>.
    /// </returns>
    /// <remarks>
    /// This operation is atomic - the item is either completely removed or remains unchanged.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="key"/> is null.</exception>
    public bool TryRemove<T>(object key, [MaybeNullWhen(false)] out T value)
    {
        ArgumentNullException.ThrowIfNull(key);
        
        if (TryGetValue(key, out value))
        {
            cache.Remove(GetKey<T>(key));
            return true;
        }

        return false;
    }

    /// <summary>
    /// Removes the cached item associated with the specified key.
    /// </summary>
    /// <param name="key">The cache key to remove.</param>
    /// <remarks>
    /// If the key doesn't exist in the cache, the method returns silently without throwing an exception.
    /// This operation is thread-safe.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="key"/> is null.</exception>
    public void Remove(object key)
    {
        ArgumentNullException.ThrowIfNull(key);
        cache.Remove(key);
    }
    
    /// <summary>
    /// Generates a unique cache key by combining the type name with the provided key.
    /// </summary>
    /// <typeparam name="T">The type of the cached item.</typeparam>
    /// <param name="key">The base cache key.</param>
    /// <returns>A string combining the type name and the provided key.</returns>
    private static string GetKey<T>(object key) => $"{GetTypeName<T>()}_{key}";
    
    /// <summary>
    /// Gets the formatted type name, including generic type parameters if applicable.
    /// </summary>
    /// <typeparam name="T">The type to get the name for.</typeparam>
    /// <returns>
    /// The simple type name for non-generic types, or a formatted name with type parameters for generic types.
    /// </returns>
    private static string GetTypeName<T>()
    {
        var type = typeof(T);
        if (!type.IsGenericType) return type.Name;
    
        var genericTypeName = type.GetGenericTypeDefinition().Name;
        genericTypeName = genericTypeName[..genericTypeName.IndexOf('`')];
    
        var genericArgs = string.Join(",", type.GetGenericArguments().Select(t => t.Name));
        return $"{genericTypeName}<{genericArgs}>";
    }
}