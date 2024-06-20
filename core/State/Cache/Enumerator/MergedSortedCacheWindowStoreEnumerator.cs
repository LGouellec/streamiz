using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Helper;

namespace Streamiz.Kafka.Net.State.Cache.Enumerator
{
    internal class MergedSortedCacheWindowStoreEnumerator :
        AbstractMergedEnumerator<long, long, byte[], byte[]> ,
        IWindowStoreEnumerator<byte[]>
    {
        private readonly ICacheFunction _cacheFunction;
        private readonly Func<byte[], long> _extractStoreTimestamp;

        public MergedSortedCacheWindowStoreEnumerator(
            ICacheFunction cacheFunction,
            IKeyValueEnumerator<Bytes, CacheEntryValue> cacheEnumerator,
            IKeyValueEnumerator<long, byte[]> storeEnumerator,
            bool forward) 
            : this(cacheFunction, cacheEnumerator, storeEnumerator, forward, WindowKeyHelper.ExtractStoreTimestamp)
        {

        }

        private MergedSortedCacheWindowStoreEnumerator(
            ICacheFunction cacheFunction,
            IKeyValueEnumerator<Bytes, CacheEntryValue> cacheEnumerator,
            IKeyValueEnumerator<long, byte[]> storeEnumerator,
            bool forward,
            Func<byte[], long> extractStoreTimestamp) 
            : base(cacheEnumerator, storeEnumerator, forward)
        {
            _cacheFunction = cacheFunction;
            _extractStoreTimestamp = extractStoreTimestamp;
        }

        protected override int Compare(Bytes cacheKey, long storeKey)
        {
            byte[] binaryKey = _cacheFunction.BytesFromCacheKey(cacheKey);
            long cacheTimestamp = _extractStoreTimestamp(binaryKey);
            return cacheTimestamp.CompareTo(storeKey);
        }

        protected override long DeserializeStoreKey(long storeKey)
            => storeKey;

        protected override KeyValuePair<long, byte[]> DeserializeStorePair(KeyValuePair<long, byte[]> pair)
            => pair;

        protected override long DeserializeCacheKey(Bytes cacheKey)
        {
            byte[] binaryKey = _cacheFunction.BytesFromCacheKey(cacheKey);
            return _extractStoreTimestamp(binaryKey);
        }

        protected override byte[] DeserializeCacheValue(CacheEntryValue cacheEntry)
            => cacheEntry.Value;
    }
}