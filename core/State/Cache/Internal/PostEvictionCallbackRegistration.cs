// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// This is a fork from Microsoft.Extensions.Caching.Memory.MemoryCache https://github.com/dotnet/runtime/blob/main/src/libraries/Microsoft.Extensions.Caching.Memory
// The only difference is the compaction process and eviction callback is synchronous whereas the .NET repo is asyncrhonous

namespace Streamiz.Kafka.Net.State.Cache.Internal
{
    /// <summary>
    /// Represents a callback delegate that will be fired after an entry is evicted from the cache.
    /// </summary>
    internal class PostEvictionCallbackRegistration<K, V>
        where K : class
        where V : class
    {
        /// <summary>
        /// Gets or sets the callback delegate that will be fired after an entry is evicted from the cache.
        /// </summary>
        public PostEvictionDelegate<K, V> EvictionCallback { get; set; }

        /// <summary>
        /// Gets or sets the state to pass to the callback delegate.
        /// </summary>
        public MemoryCache<K, V>? State { get; set; }
    }
}
