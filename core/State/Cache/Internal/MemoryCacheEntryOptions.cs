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
    /// Represents the cache options applied to an entry of the <see cref="IMemoryCache"/> instance.
    /// </summary>
    internal class MemoryCacheEntryOptions<K, V>
        where K : class
        where V : class
    {
        private long? _size;
        

        /// <summary>
        /// Gets or sets the callbacks will be fired after the cache entry is evicted from the cache.
        /// </summary>
        public IList<PostEvictionCallbackRegistration<K, V>> PostEvictionCallbacks { get; }
            = new List<PostEvictionCallbackRegistration<K, V>>();

        /// <summary>
        /// Gets or sets the size of the cache entry value.
        /// </summary>
        public long? Size
        {
            get => _size;
            set
            {
                if (value < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), value, $"{nameof(value)} must be non-negative.");
                }

                _size = value;
            }
        }
    }
}
