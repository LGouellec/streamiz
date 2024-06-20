// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// This is a fork from Microsoft.Extensions.Caching.Memory.MemoryCache https://github.com/dotnet/runtime/blob/main/src/libraries/Microsoft.Extensions.Caching.Memory
// The only difference is the compaction process and eviction callback is synchronous whereas the .NET repo is asyncrhonous

namespace Streamiz.Kafka.Net.State.Cache.Internal
{
    /// <summary>
    /// Holds a snapshot of statistics for a memory cache.
    /// </summary>
    internal class MemoryCacheStatistics
    {
        /// <summary>
        /// Initializes an instance of MemoryCacheStatistics.
        /// </summary>
        public MemoryCacheStatistics() { }

        /// <summary>
        /// Gets the number of <see cref="ICacheEntry" /> instances currently in the memory cache.
        /// </summary>
        public long CurrentEntryCount { get; set; }

        /// <summary>
        /// Gets an estimated sum of all the <see cref="ICacheEntry.Size" /> values currently in the memory cache.
        /// </summary>
        /// <returns>Returns <see langword="null"/> if size isn't being tracked. The common MemoryCache implementation tracks size whenever a SizeLimit is set on the cache.</returns>
        public long? CurrentEstimatedSize { get; set; }

        /// <summary>
        /// Gets the total number of cache misses.
        /// </summary>
        public long TotalMisses { get; set; }

        /// <summary>
        /// Gets the total number of cache hits.
        /// </summary>
        public long TotalHits { get; set; }
    }
}
