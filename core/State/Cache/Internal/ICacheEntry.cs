// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// This is a fork from Microsoft.Extensions.Caching.Memory.MemoryCache https://github.com/dotnet/runtime/blob/main/src/libraries/Microsoft.Extensions.Caching.Memory
// The only difference is the compaction process and eviction callback is synchronous whereas the .NET repo is asyncrhonous

using System;
using System.Collections.Generic;
using Microsoft.Extensions.Primitives;

namespace Streamiz.Kafka.Net.State.Cache.Internal
{
    /// <summary>
    /// Represents an entry in the <see cref="IMemoryCache"/> implementation.
    /// When Disposed, is committed to the cache.
    /// </summary>
    public interface ICacheEntry<K, V> : IDisposable
    {
        /// <summary>
        /// Gets the key of the cache entry.
        /// </summary>
        K Key { get; }

        /// <summary>
        /// Gets or set the value of the cache entry.
        /// </summary>
        V Value { get; set; }

        /// <summary>
        /// Gets or sets the callbacks will be fired after the cache entry is evicted from the cache.
        /// </summary>
        IList<PostEvictionCallbackRegistration> PostEvictionCallbacks { get; }

        /// <summary>
        /// Gets or sets the priority for keeping the cache entry in the cache during a
        ///  cleanup. The default is <see cref="CacheItemPriority.Normal"/>.
        /// </summary>
        CacheItemPriority Priority { get; set; }

        /// <summary>
        /// Gets or set the size of the cache entry value.
        /// </summary>
        long? Size { get; set; }
    }
}
