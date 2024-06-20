// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// This is a fork from Microsoft.Extensions.Caching.Memory.MemoryCache https://github.com/dotnet/runtime/blob/main/src/libraries/Microsoft.Extensions.Caching.Memory
// The only difference is the compaction process and eviction callback is synchronous whereas the .NET repo is asyncrhonous

using System;

namespace Streamiz.Kafka.Net.State.Cache.Internal
{
    /// <summary>
    /// Represents a local in-memory cache whose values are not serialized.
    /// </summary>
    internal interface IMemoryCache<K, V> : IDisposable
        where K : class
        where V : class
    {
        /// <summary>
        /// Gets the item associated with this key if present.
        /// </summary>
        /// <param name="key">An object identifying the requested entry.</param>
        /// <param name="value">The located value or null.</param>
        /// <returns>True if the key was found.</returns>
        bool TryGetValue(K key, out V value);

        /// <summary>
        /// Create or overwrite an entry in the cache.
        /// </summary>
        /// <param name="key">An object identifying the entry.</param>
        /// <returns>The newly created <see cref="ICacheEntry"/> instance.</returns>
        ICacheEntry<K, V> CreateEntry(K key);

        /// <summary>
        /// Removes the object associated with the given key.
        /// </summary>
        /// <param name="key">An object identifying the entry.</param>
        void Remove(K key);
        
        /// <summary>
        /// Gets a snapshot of the cache statistics if available.
        /// </summary>
        /// <returns>An instance of <see cref="MemoryCacheStatistics"/> containing a snapshot of the cache statistics.</returns>
        MemoryCacheStatistics GetCurrentStatistics();
    }
}
