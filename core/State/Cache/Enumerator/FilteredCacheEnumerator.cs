using System;
using System.Collections;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.State.Cache.Enumerator
{
    internal class FilteredCacheEnumerator : IKeyValueEnumerator<Bytes, CacheEntryValue>
    {
        private class WrappredFilteredCacheEnumerator : IKeyValueEnumerator<Bytes, byte[]>
        {
            private readonly ICacheFunction _cacheFunction;
            private readonly IKeyValueEnumerator<Bytes, CacheEntryValue> _cacheEnumerator;

            public WrappredFilteredCacheEnumerator(
                ICacheFunction cacheFunction,
                IKeyValueEnumerator<Bytes,CacheEntryValue> cacheEnumerator)
            {
                _cacheFunction = cacheFunction;
                _cacheEnumerator = cacheEnumerator;
            }
            
            private KeyValuePair<Bytes, byte[]> CachedPair(KeyValuePair<Bytes, CacheEntryValue> next){
                return new KeyValuePair<Bytes, byte[]>(_cacheFunction.Key(next.Key), next.Value.Value);
            }

            public Bytes PeekNextKey() => _cacheFunction.Key(_cacheEnumerator.PeekNextKey());

            public bool MoveNext() => _cacheEnumerator.MoveNext();

            public void Reset() => _cacheEnumerator.Reset();

            public KeyValuePair<Bytes, byte[]>? Current
                => _cacheEnumerator.Current is not null
                    ? CachedPair(_cacheEnumerator.Current.Value)
                    : null;

            object IEnumerator.Current => Current;

            public void Dispose() => _cacheEnumerator.Dispose();
        }
        
        private readonly IKeyValueEnumerator<Bytes, CacheEntryValue> _cacheEnumerator;
        private readonly IKeyValueEnumerator<Bytes, byte[]> _wrappedCacheEnumerator;
        private readonly Func<IKeyValueEnumerator<Bytes, byte[]>, bool> _hasNextCondition;

        internal FilteredCacheEnumerator(
            IKeyValueEnumerator<Bytes,CacheEntryValue> cacheEnumerator,
            Func<IKeyValueEnumerator<Bytes, byte[]>,bool> hasNextCondition,
            ICacheFunction segmentCacheFunction)
        {
            _cacheEnumerator = cacheEnumerator;
            _hasNextCondition = hasNextCondition;
            _wrappedCacheEnumerator = new WrappredFilteredCacheEnumerator(segmentCacheFunction, _cacheEnumerator);
        }

        public Bytes PeekNextKey() => _cacheEnumerator.PeekNextKey();

        public bool MoveNext() => _hasNextCondition(_wrappedCacheEnumerator);

        public void Reset()
            => _cacheEnumerator.Reset();

        public KeyValuePair<Bytes, CacheEntryValue>? Current => _cacheEnumerator.Current;

        object IEnumerator.Current => Current;

        public void Dispose()
            => _cacheEnumerator.Dispose();
    }
}