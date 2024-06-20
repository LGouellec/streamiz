using System;
using System.Collections.Generic;
using System.Reflection;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.State.Cache.Enumerator
{
    internal class MergedSortedCacheWindowStoreKeyValueEnumerator :
        AbstractMergedEnumerator<Windowed<Bytes>, Windowed<Bytes>, byte[], byte[]> {
        
        private readonly long _windowSize;
        private readonly ICacheFunction _cacheFunction;
        private readonly ISerDes<Bytes> _serdes;
        private readonly string _changelogTopic;
        private readonly Func<byte[], long, ISerDes<Bytes>, string, Windowed<Bytes>> _storeKeyToWindowKey;
        private readonly Func<Bytes, long, int, Bytes> _windowKeyToBytes;

        public MergedSortedCacheWindowStoreKeyValueEnumerator(
            IKeyValueEnumerator<Bytes, CacheEntryValue> cacheEnumerator,
            IKeyValueEnumerator<Windowed<Bytes>, byte[]> storeEnumerator,
            long windowSize,
            ICacheFunction cacheFunction,
            ISerDes<Bytes> serdes,
            string changelogTopic,
            Func<byte[], long, ISerDes<Bytes>, string, Windowed<Bytes>> storeKeyToWindowKey,
            Func<Bytes, long, int, Bytes> windowKeyToBytes, 
                bool forward) 
            : base(cacheEnumerator, storeEnumerator, forward)
        {
            _windowSize = windowSize;
            _cacheFunction = cacheFunction;
            _serdes = serdes;
            _changelogTopic = changelogTopic;
            _storeKeyToWindowKey = storeKeyToWindowKey;
            _windowKeyToBytes = windowKeyToBytes;
        }

        protected override int Compare(Bytes cacheKey, Windowed<Bytes> storeKey)
        {
            var storeKeyBytes = _windowKeyToBytes(storeKey.Key, storeKey.Window.StartMs, 0);
            return _cacheFunction.CompareSegmentedKeys(cacheKey, storeKeyBytes);
        }

        protected override Windowed<Bytes> DeserializeStoreKey(Windowed<Bytes> storeKey) => storeKey;

        protected override KeyValuePair<Windowed<Bytes>, byte[]> DeserializeStorePair(
            KeyValuePair<Windowed<Bytes>, byte[]> pair)
            => pair;

        protected override Windowed<Bytes> DeserializeCacheKey(Bytes cacheKey)
        {
            var byteKey = _cacheFunction.Key(cacheKey).Get;
            return _storeKeyToWindowKey(byteKey, _windowSize, _serdes, _changelogTopic);
        }

        protected override byte[] DeserializeCacheValue(CacheEntryValue cacheEntry)
            => cacheEntry.Value;
    }
}