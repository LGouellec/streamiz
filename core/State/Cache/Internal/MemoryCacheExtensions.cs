// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// This is a fork from Microsoft.Extensions.Caching.Memory.MemoryCache https://github.com/dotnet/runtime/blob/main/src/libraries/Microsoft.Extensions.Caching.Memory
// The only difference is the compaction process and eviction callback is synchronous whereas the .NET repo is asyncrhonous

using System;

namespace Streamiz.Kafka.Net.State.Cache.Internal
{
    /// <summary>
    /// Provide extensions methods for <see cref="IMemoryCache{K, V}"/> operations.
    /// </summary>
    internal static class CacheExtensions
    {
        /// <summary>
        /// Gets the value associated with this key if present.
        /// </summary>
        /// <param name="cache">The <see cref="IMemoryCache{K, V}"/> instance this method extends.</param>
        /// <param name="key">The key of the value to get.</param>
        /// <returns>The value associated with this key, or <c>null</c> if the key is not present.</returns>
        internal static V Get<K, V>(this IMemoryCache<K, V> cache, K key)
            where K : class
            where V : class
        {
            cache.TryGetValue(key, out V value);
            return value;
        }

        /// <summary>
        /// Try to get the value associated with the given key.
        /// </summary>
        /// <param name="cache">The <see cref="IMemoryCache{K, V}"/> instance this method extends.</param>
        /// <param name="key">The key of the value to get.</param>
        /// <param name="value">The value associated with the given key.</param>
        /// <returns><c>true</c> if the key was found. <c>false</c> otherwise.</returns>
        internal static bool TryGetValue<K, V>(this IMemoryCache<K, V> cache, K key, out V value)
            where K : class
            where V : class
        {
            if (cache.TryGetValue(key, out V result))
            {
                if (result == null)
                {
                    value = default;
                    return false; // if result == null, is a delete
                }

                if (result is { } item)
                {
                    value = item;
                    return true;
                }
            }

            value = default;
            return false;
        }

        /// <summary>
        /// Associate a value with a key in the <see cref="IMemoryCache{K, V}"/>.
        /// </summary>
        /// <param name="cache">The <see cref="IMemoryCache{K, V}"/> instance this method extends.</param>
        /// <param name="key">The key of the entry to add.</param>
        /// <param name="value">The value to associate with the key.</param>
        /// <returns>The value that was set.</returns>
        internal static V Set<K, V>(this IMemoryCache<K, V> cache, K key, V value)
            where K : class
            where V : class
        {
            using ICacheEntry<K, V> entry = cache.CreateEntry(key);
            entry.Value = value;

            return value;
        }
        

        /// <summary>
        /// Sets a cache entry with the given key and value and apply the values of an existing <see cref="MemoryCacheEntryOptions{K, V}"/> to the created entry.
        /// </summary>
        /// <param name="cache">The <see cref="IMemoryCache{K, V}"/> instance this method extends.</param>
        /// <param name="key">The key of the entry to add.</param>
        /// <param name="value">The value to associate with the key.</param>
        /// <param name="options">The existing <see cref="MemoryCacheEntryOptions{K, V}"/> instance to apply to the new entry.</param>
        /// <param name="reason">Initial eviction reason</param>
        /// <returns>The value that was set.</returns>
        internal static V Set<K, V>(this IMemoryCache<K, V> cache, K key, V value, MemoryCacheEntryOptions<K, V> options, EvictionReason reason)
            where K : class
            where V : class
        {
            using ICacheEntry<K, V> entry = cache.CreateEntry(key);
            if (options != null)
            {
                entry.SetOptions(options);
            }
            
            entry.Value = value;
            entry.SetExpired(reason);

            return value;
        }

        /// <summary>
        /// Gets the value associated with this key if it exists, or generates a new entry using the provided key and a value from the given factory if the key is not found.
        /// </summary>
        /// <param name="cache">The <see cref="IMemoryCache{K, V}"/> instance this method extends.</param>
        /// <param name="key">The key of the entry to look for or create.</param>
        /// <param name="factory">The factory that creates the value associated with this key if the key does not exist in the cache.</param>
        /// <returns>The value associated with this key.</returns>
        internal static V GetOrCreate<K, V>(this IMemoryCache<K, V> cache, K key, Func<ICacheEntry<K, V>, V> factory)
            where K : class
            where V : class
        {
            return GetOrCreate(cache, key, factory, null);
        }

        /// <summary>
        /// Gets the value associated with this key if it exists, or generates a new entry using the provided key and a value from the given factory if the key is not found.
        /// </summary>
        /// <param name="cache">The <see cref="IMemoryCache{K, V}"/> instance this method extends.</param>
        /// <param name="key">The key of the entry to look for or create.</param>
        /// <param name="factory">The factory that creates the value associated with this key if the key does not exist in the cache.</param>
        /// <param name="createOptions">The options to be applied to the <see cref="ICacheEntry{K, V}"/> if the key does not exist in the cache.</param>
        internal static V GetOrCreate<K, V>(this IMemoryCache<K, V> cache, K key, Func<ICacheEntry<K, V>, V> factory, MemoryCacheEntryOptions<K, V>? createOptions)
            where K : class
            where V : class
        {
            if (!cache.TryGetValue(key, out V result))
            {
                using ICacheEntry<K, V> entry = cache.CreateEntry(key);

                if (createOptions != null)
                {
                    entry.SetOptions(createOptions);
                }

                result = factory(entry);
                entry.Value = result;
            }

            return result;
        }
    }
}
