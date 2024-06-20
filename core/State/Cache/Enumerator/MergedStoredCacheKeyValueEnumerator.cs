using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.State.Cache.Enumerator
{
    internal class MergedStoredCacheKeyValueEnumerator : 
        AbstractMergedEnumerator<Bytes, Bytes, byte[], byte[]>
    {
        public MergedStoredCacheKeyValueEnumerator(
            IKeyValueEnumerator<Bytes, CacheEntryValue> cacheEnumerator,
            IKeyValueEnumerator<Bytes, byte[]> storeEnumerator,
            bool forward) 
            : base(cacheEnumerator, storeEnumerator, forward)
        {
        }

        protected override int Compare(Bytes cacheKey, Bytes storeKey)
            => cacheKey.CompareTo(storeKey);

        protected override Bytes DeserializeStoreKey(Bytes storeKey)
            => storeKey;

        protected override KeyValuePair<Bytes, byte[]> DeserializeStorePair(KeyValuePair<Bytes, byte[]> pair)
            => pair;

        protected override Bytes DeserializeCacheKey(Bytes cacheKey)
            => cacheKey;

        protected override byte[] DeserializeCacheValue(CacheEntryValue cacheEntry)
            => cacheEntry.Value;
    }
}