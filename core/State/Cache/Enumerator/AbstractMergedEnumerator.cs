using System;
using System.Collections;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.State.Cache.Enumerator
{
    internal abstract class AbstractMergedEnumerator<K, KS, V, VS> : IKeyValueEnumerator<K, V>
    {
        private enum LastChoice
        {
            NONE,
            STORE,
            CACHE,
            BOTH
        };
        
        private readonly bool forward;
        private readonly IKeyValueEnumerator<KS, VS> storeEnumerator;
        private readonly IKeyValueEnumerator<Bytes, CacheEntryValue> cacheEnumerator;
        private LastChoice _lastChoice = LastChoice.NONE;
        
        protected AbstractMergedEnumerator(
            IKeyValueEnumerator<Bytes, CacheEntryValue> cacheEnumerator, 
            IKeyValueEnumerator<KS, VS> storeEnumerator,
            bool forward)
        {
            this.storeEnumerator = storeEnumerator;
            this.cacheEnumerator = cacheEnumerator;
            this.forward = forward;
        }
            
        private bool IsDeletedCacheEntry(KeyValuePair<Bytes, CacheEntryValue>? nextFromCache) 
            => nextFromCache?.Value.Value == null;

        public K PeekNextKey() => Current.Value.Key;

        public bool MoveNext()
        {
            // advance the store enumerator if choice is NONE ou STORE
            if(_lastChoice is LastChoice.NONE or LastChoice.STORE or LastChoice.BOTH)
                storeEnumerator.MoveNext();

            if (_lastChoice is LastChoice.NONE or LastChoice.CACHE or LastChoice.BOTH)
            {
                // skip over items deleted from cache, and corresponding store items if they have the same key
                while (cacheEnumerator.MoveNext() && IsDeletedCacheEntry(cacheEnumerator.Current))
                {
                    var currentKeyStore = storeEnumerator.Current;
                    // advance the store enumerator if the key is the same as the deleted cache key
                    if (currentKeyStore != null &&
                        cacheEnumerator.Current != null &&
                        Compare(cacheEnumerator.Current.Value.Key, currentKeyStore.Value.Key) == 0)
                        storeEnumerator.MoveNext();
                }
            }

            Bytes nextCacheKey = cacheEnumerator.Current?.Key;
            bool nullStoreKey = !storeEnumerator.Current.HasValue;
                
            if (nextCacheKey == null) {
                Current = CurrentStoreValue();
            }
            else if (nullStoreKey) {
                Current = CurrentCacheValue();
            }
            else
            {
                int comparison = Compare(nextCacheKey, storeEnumerator.Current.Value.Key);
                Current = ChooseCurrentValue(comparison);
            }

            return Current != null;
        }

        public void Reset()
        {
            Current = null;
            cacheEnumerator.Reset();
            storeEnumerator.Reset();
        }

        public KeyValuePair<K, V>? Current { get; private set; }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            cacheEnumerator.Dispose();
            storeEnumerator.Dispose();
            GC.SuppressFinalize(this);
        }
        
        #region Abstract

        protected abstract int Compare(Bytes cacheKey, KS storeKey);
        protected abstract K DeserializeStoreKey(KS storeKey);
        protected abstract KeyValuePair<K, V> DeserializeStorePair(KeyValuePair<KS, VS> pair);
        protected abstract K DeserializeCacheKey(Bytes cacheKey);
        protected abstract V DeserializeCacheValue(CacheEntryValue cacheEntry);

        #endregion
        
        private KeyValuePair<K, V>? ChooseCurrentValue(int comparison)
        {
            if (forward)
            {
                if (comparison > 0) {
                    return CurrentStoreValue();
                }

                if (comparison < 0) {
                    return CurrentCacheValue();
                }

                return CurrentCacheValue(true);
            }

            if (comparison < 0) {
                return CurrentStoreValue();
            }

            if (comparison > 0) {
                return CurrentCacheValue();
            }

            return CurrentCacheValue(true);
        }
        
        private KeyValuePair<K, V>? CurrentStoreValue()
        {
            if (storeEnumerator.Current is not null)
            {
                _lastChoice = LastChoice.STORE;
                return DeserializeStorePair(storeEnumerator.Current.Value);
            }

            return null;
        }
        
        private KeyValuePair<K, V>? CurrentCacheValue(bool storeEqual = false)
        {
            if (cacheEnumerator.Current is not null)
            {
                KeyValuePair<Bytes, CacheEntryValue> next = cacheEnumerator.Current.Value;
                _lastChoice = !storeEqual ? LastChoice.CACHE : LastChoice.BOTH;
                return new KeyValuePair<K, V>(DeserializeCacheKey(next.Key), DeserializeCacheValue(next.Value));
            }

            return null;
        }
    }
}