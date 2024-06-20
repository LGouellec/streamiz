// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// This is a fork from Microsoft.Extensions.Caching.Memory.MemoryCache https://github.com/dotnet/runtime/blob/main/src/libraries/Microsoft.Extensions.Caching.Memory
// The only difference is the compaction process and eviction callback is synchronous whereas the .NET repo is asyncrhonous

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.State.Cache.Internal
{
    internal sealed partial class CacheEntry<K, V>
    {
        // this type exists just to reduce average CacheEntry size
        // which typically is not using expiration tokens or callbacks
        private sealed class CacheEntryTokens
        {
            private List<PostEvictionCallbackRegistration<K, V>>? _postEvictionCallbacks; // this is not really related to tokens, but was moved here to shrink typical CacheEntry size

            internal List<PostEvictionCallbackRegistration<K, V>> PostEvictionCallbacks => _postEvictionCallbacks ??= new List<PostEvictionCallbackRegistration<K, V>>();
            

            internal void InvokeEvictionCallbacks(CacheEntry<K, V> cacheEntry)
            {
                if (_postEvictionCallbacks != null)
                {
                    InvokeCallbacks(cacheEntry);
                }
            }

            private void InvokeCallbacks(CacheEntry<K, V> entry)
            {
                Debug.Assert(entry._tokens != null);
                List<PostEvictionCallbackRegistration<K, V>>? callbackRegistrations = Interlocked.Exchange(ref entry._tokens._postEvictionCallbacks, null);

                if (callbackRegistrations == null)
                {
                    return;
                }

                for (int i = 0; i < callbackRegistrations.Count; i++)
                {
                    PostEvictionCallbackRegistration<K, V> registration = callbackRegistrations[i];

                    try
                    {
                        registration.EvictionCallback?.Invoke(entry.Key, entry.Value, entry.EvictionReason, registration.State);
                    }
                    catch (Exception e)
                    {
                        // This will be invoked on a background thread, don't let it throw.
                        entry._cache.Logger.LogError(e, "EvictionCallback invoked failed");
                    }
                }
            }
        }
    }
}
