// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// This is a fork from Microsoft.Extensions.Caching.Memory.MemoryCache https://github.com/dotnet/runtime/blob/main/src/libraries/Microsoft.Extensions.Caching.Memory
// The only difference is the compaction process and eviction callback is synchronous whereas the .NET repo is asyncrhonous

using System;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Primitives;

namespace Streamiz.Kafka.Net.State.Cache.Internal
{
    /// <summary>
    /// Provide extensions methods for <see cref="ICacheEntry"/> operations.
    /// </summary>
    public static class CacheEntryExtensions
    {
        /// <summary>
        /// Sets the priority for keeping the cache entry in the cache during a memory pressure tokened cleanup.
        /// </summary>
        /// <param name="entry">The entry to set the priority for.</param>
        /// <param name="priority">The <see cref="CacheItemPriority"/> to set on the entry.</param>
        /// <returns>The <see cref="ICacheEntry"/> for chaining.</returns>
        public static ICacheEntry<K, V> SetPriority<K, V>(
            this ICacheEntry<K, V> entry,
            CacheItemPriority priority)
        {
            entry.Priority = priority;
            return entry;
        }

        /// <summary>
        /// The given callback will be fired after the cache entry is evicted from the cache.
        /// </summary>
        /// <param name="entry">The <see cref="ICacheEntry"/>.</param>
        /// <param name="callback">The callback to run after the entry is evicted.</param>
        /// <returns>The <see cref="ICacheEntry"/> for chaining.</returns>
        public static ICacheEntry<K, V> RegisterPostEvictionCallback<K, V>(
            this ICacheEntry<K, V> entry,
            PostEvictionDelegate callback)
        {
            return entry.RegisterPostEvictionCallbackNoValidation(callback, state: null);
        }

        /// <summary>
        /// The given callback will be fired after the cache entry is evicted from the cache.
        /// </summary>
        /// <param name="entry">The <see cref="ICacheEntry"/>.</param>
        /// <param name="callback">The callback to run after the entry is evicted.</param>
        /// <param name="state">The state to pass to the post-eviction callback.</param>
        /// <returns>The <see cref="ICacheEntry"/> for chaining.</returns>
        public static ICacheEntry<K, V> RegisterPostEvictionCallback<K, V>(
            this ICacheEntry<K, V> entry,
            PostEvictionDelegate callback,
            object? state)
        {
            return entry.RegisterPostEvictionCallbackNoValidation(callback, state);
        }

        private static ICacheEntry<K, V> RegisterPostEvictionCallbackNoValidation<K, V>(
            this ICacheEntry<K, V> entry,
            PostEvictionDelegate callback,
            object? state)
        {
            entry.PostEvictionCallbacks.Add(new PostEvictionCallbackRegistration()
            {
                EvictionCallback = callback,
                State = state
            });
            return entry;
        }

        /// <summary>
        /// Sets the value of the cache entry.
        /// </summary>
        /// <param name="entry">The <see cref="ICacheEntry"/>.</param>
        /// <param name="value">The value to set on the <paramref name="entry"/>.</param>
        /// <returns>The <see cref="ICacheEntry"/> for chaining.</returns>
        public static ICacheEntry<K, V> SetValue<K, V>(
            this ICacheEntry<K, V> entry,
            V value)
        {
            entry.Value = value;
            return entry;
        }

        /// <summary>
        /// Sets the size of the cache entry value.
        /// </summary>
        /// <param name="entry">The <see cref="ICacheEntry"/>.</param>
        /// <param name="size">The size to set on the <paramref name="entry"/>.</param>
        /// <returns>The <see cref="ICacheEntry"/> for chaining.</returns>
        public static ICacheEntry<K, V> SetSize<K, V>(
            this ICacheEntry<K, V> entry,
            long size)
        {
            if (size < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(size), size, $"{nameof(size)} must be non-negative.");
            }

            entry.Size = size;
            return entry;
        }

        /// <summary>
        /// Applies the values of an existing <see cref="MemoryCacheEntryOptions"/> to the entry.
        /// </summary>
        /// <param name="entry">The <see cref="ICacheEntry"/>.</param>
        /// <param name="options">Set the values of these options on the <paramref name="entry"/>.</param>
        /// <returns>The <see cref="ICacheEntry"/> for chaining.</returns>
        public static ICacheEntry<K, V> SetOptions<K, V>(this ICacheEntry<K, V> entry, MemoryCacheEntryOptions options)
        {
            entry.Priority = options.Priority;
            entry.Size = options.Size;

            for (int i = 0; i < options.PostEvictionCallbacks.Count; i++)
            {
                PostEvictionCallbackRegistration postEvictionCallback = options.PostEvictionCallbacks[i];
                if (postEvictionCallback.EvictionCallback is null)
                    ThrowNullCallback(i, nameof(options));

                entry.RegisterPostEvictionCallbackNoValidation(postEvictionCallback.EvictionCallback, postEvictionCallback.State);
            }

            return entry;
        }

        private static void ThrowNullCallback(int index, string paramName)
        {
            string message =
                $"MemoryCacheEntryOptions.PostEvictionCallbacks contains a PostEvictionCallbackRegistration with a null EvictionCallback at index {index}.";
            throw new ArgumentException(message, paramName);
        }
    }
}
