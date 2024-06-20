using System;
using System.Collections;
using System.Collections.Generic;
using Streamiz.Kafka.Net.State.Cache.Internal;
using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.State.Cache.Enumerator
{
    internal class CacheEnumerator<K, V> : IKeyValueEnumerator<K, V>
        where K : class
        where V : class
    {
        private IEnumerator<K> keys;
        private readonly MemoryCache<K, V> cache;
        private readonly Action _beforeClosing;
        private KeyValuePair<K, V>? current;
        
        public CacheEnumerator(
            IEnumerable<K> keys,
            MemoryCache<K, V> cache,
            Action beforeClosing)
        {
            this.keys = keys.GetEnumerator();
            this.cache = cache;
            _beforeClosing = beforeClosing;
        }
        
        public K PeekNextKey()
            => current?.Key;

        public bool MoveNext()
        {
            var result = keys.MoveNext();
            if (result)
                current = new KeyValuePair<K, V>(keys.Current, cache.Get(keys.Current));
            else
                current = null;
            return result;
        }

        public void Reset()
        {
            keys.Reset();
        }

        public KeyValuePair<K, V>? Current => current;

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            current = null;
            keys.Dispose();
            _beforeClosing?.Invoke();
        }
    }
}