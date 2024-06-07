// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// This is a fork from Microsoft.Extensions.Caching.Memory.MemoryCache https://github.com/dotnet/runtime/blob/main/src/libraries/Microsoft.Extensions.Caching.Memory
// The only difference is the compaction process and eviction callback is synchronous whereas the .NET repo is asyncrhonous

namespace Streamiz.Kafka.Net.State.Cache.Internal
{
    /// <summary>
    /// Specify the reasons why an entry was evicted from the cache.
    /// </summary>
    internal enum EvictionReason
    {
        /// <summary>
        /// The item was not removed from the cache or getting from the internal wrapped store
        /// </summary>
        None,
        
        /// <summary>
        /// The item was adding to the cache.
        /// </summary>
        Setted,

        /// <summary>
        /// The item was removed from the cache manually.
        /// </summary>
        Removed,

        /// <summary>
        /// The item was removed from the cache because it was overwritten.
        /// </summary>
        Replaced,

        /// <summary>
        /// The item was removed from the cache because it exceeded its capacity.
        /// </summary>
        Capacity,
    }
}
