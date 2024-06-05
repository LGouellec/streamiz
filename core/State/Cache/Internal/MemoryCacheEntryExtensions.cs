// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// This is a fork from Microsoft.Extensions.Caching.Memory.MemoryCache https://github.com/dotnet/runtime/blob/main/src/libraries/Microsoft.Extensions.Caching.Memory
// The only difference is the compaction process and eviction callback is synchronous whereas the .NET repo is asyncrhonous

using System;

namespace Streamiz.Kafka.Net.State.Cache.Internal
{
    /// <summary>
    /// Provide extensions methods for <see cref="MemoryCacheEntryOptions"/> operations.
    /// </summary>
    internal static class MemoryCacheEntryExtensions
    {
        /// <summary>
        /// Sets the size of the cache entry value.
        /// </summary>
        /// <param name="options">The options to set the entry size on.</param>
        /// <param name="size">The size to set on the <see cref="MemoryCacheEntryOptions"/>.</param>
        /// <returns>The <see cref="MemoryCacheEntryOptions"/> so that additional calls can be chained.</returns>
        internal static MemoryCacheEntryOptions<K, V> SetSize<K, V>(
            this MemoryCacheEntryOptions<K, V>  options,
            long size)
            where K : class
            where V : class
        {
            if (size < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(size), size, $"{nameof(size)} must be non-negative.");
            }

            options.Size = size;
            return options;
        }
        
        /// <summary>
        /// The given callback will be fired after the cache entry is evicted from the cache.
        /// </summary>
        /// <param name="options">The <see cref="MemoryCacheEntryOptions"/>.</param>
        /// <param name="callback">The callback to register for calling after an entry is evicted.</param>
        /// <param name="state">The state to pass to the callback.</param>
        /// <returns>The <see cref="MemoryCacheEntryOptions"/> so that additional calls can be chained.</returns>
        internal static MemoryCacheEntryOptions<K, V> RegisterPostEvictionCallback<K, V>(
            this MemoryCacheEntryOptions<K, V> options,
            PostEvictionDelegate<K, V> callback,
            MemoryCache<K, V> state)
            where K : class
            where V : class
        {
            options.PostEvictionCallbacks.Add(new PostEvictionCallbackRegistration<K, V>()
            {
                EvictionCallback = callback,
                State = state
            });
            return options;
        }
    }
}
