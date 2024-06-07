// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// This is a fork from Microsoft.Extensions.Caching.Memory.MemoryCache https://github.com/dotnet/runtime/blob/main/src/libraries/Microsoft.Extensions.Caching.Memory
// The only difference is the compaction process and eviction callback is synchronous whereas the .NET repo is asyncrhonous

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Streamiz.Kafka.Net.State.Cache.Internal
{
    internal sealed partial class CacheEntry<K, V> : ICacheEntry<K, V>
        where K : class
        where V : class
    {
        //private static readonly Action<object> ExpirationCallback = ExpirationTokensExpired;
        private static readonly AsyncLocal<CacheEntry<K, V>?> _current = new();

        private readonly MemoryCache<K, V> _cache;

        private CacheEntryTokens? _tokens; // might be null if user is not using the tokens or callbacks
        private long _size = NotSet;
        private V _value;
        private bool _isDisposed;
        private bool _isValueSet;
        private byte _evictionReason;
        private bool _isExpired;
        
        private const int NotSet = -1;

        internal CacheEntry(K key, MemoryCache<K, V> memoryCache)
        {
            Key = key;
            _cache = memoryCache;
        }

        // internal for testing
        internal static CacheEntry<K, V>? Current => _current.Value;
        
        
        /// <summary>
        /// Gets or sets the callbacks will be fired after the cache entry is evicted from the cache.
        /// </summary>
        public IList<PostEvictionCallbackRegistration<K, V>> PostEvictionCallbacks => GetOrCreateTokens().PostEvictionCallbacks;
        
        internal long Size => _size;

        long? ICacheEntry<K, V>.Size
        {
            get => _size < 0 ? null : _size;
            set
            {
                if (value < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), value, $"{nameof(value)} must be non-negative.");
                }

                _size = value ?? NotSet;
            }
        }

        public K Key { get; }

        public V Value
        {
            get => _value;
            set
            {
                _value = value;
                _isValueSet = true;
            }
        }

        internal DateTime LastAccessed { get; set; }

        internal EvictionReason EvictionReason { get => (EvictionReason)_evictionReason; private set => _evictionReason = (byte)value; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)] // added based on profiling
        internal bool CheckExpired(DateTime utcNow)
            => _isExpired;
        
        public void Dispose()
        {
            if (!_isDisposed)
            {
                _isDisposed = true;

                if (_isValueSet)
                {
                    _cache.SetEntry(this);
                }
            }
        }

        public void SetExpired(EvictionReason reason)
        {
            EvictionReason = reason;
            _isExpired = true;
        }
        
        internal void InvokeEvictionCallbacks() => _tokens?.InvokeEvictionCallbacks(this);
        
        private CacheEntryTokens GetOrCreateTokens()
        {
            if (_tokens != null)
            {
                return _tokens;
            }

            CacheEntryTokens result = new CacheEntryTokens();
            return Interlocked.CompareExchange(ref _tokens, result, null) ?? result;
        }
    }
}
