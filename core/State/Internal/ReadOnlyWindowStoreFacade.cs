using Streamiz.Kafka.Net.State.Enumerator;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class WindowStoreIteratorFacade<V> : IWindowStoreEnumerator<V>
    {
        private IWindowStoreEnumerator<ValueAndTimestamp<V>> windowStoreEnumerator;

        public WindowStoreIteratorFacade(IWindowStoreEnumerator<ValueAndTimestamp<V>> windowStoreEnumerator)
        {
            this.windowStoreEnumerator = windowStoreEnumerator;
        }

        public KeyValuePair<long, V> Current
        {
            get
            {
                var innerValue = windowStoreEnumerator.Current;
                return new KeyValuePair<long, V>(innerValue.Key, innerValue.Value.Value);
            }
        }

        object IEnumerator.Current => Current;

        public void Dispose()
            => windowStoreEnumerator.Dispose();

        public bool MoveNext()
            => windowStoreEnumerator.MoveNext();

        public long PeekNextKey()
            => windowStoreEnumerator.PeekNextKey();

        public void Reset()
            => windowStoreEnumerator.Reset();
    }

    internal class KeyValueIteratorFacade<K, V> : IKeyValueEnumerator<Windowed<K>, V>
    {
        private IKeyValueEnumerator<Windowed<K>, ValueAndTimestamp<V>> keyValueEnumerator;

        public KeyValueIteratorFacade(IKeyValueEnumerator<Windowed<K>, ValueAndTimestamp<V>> keyValueEnumerator)
        {
            this.keyValueEnumerator = keyValueEnumerator;
        }

        public KeyValuePair<Windowed<K>, V> Current
        {
            get
            {
                var innerValue = keyValueEnumerator.Current;
                return new KeyValuePair<Windowed<K>, V>(innerValue.Key, innerValue.Value != null ? innerValue.Value.Value : default);
            }
        }

        object IEnumerator.Current => Current;

        public void Dispose()
            => keyValueEnumerator.Dispose();

        public bool MoveNext()
            => keyValueEnumerator.MoveNext();

        public Windowed<K> PeekNextKey()
            => keyValueEnumerator.PeekNextKey();

        public void Reset()
            => keyValueEnumerator.Reset();
    }

    internal class ReadOnlyWindowStoreFacade<K, V> : ReadOnlyWindowStore<K, V>
    {
        private readonly TimestampedWindowStore<K, V> innerStore;

        public ReadOnlyWindowStoreFacade(TimestampedWindowStore<K, V> store)
        {
            innerStore = store;
        }

        public IKeyValueEnumerator<Windowed<K>, V> All()
            => new KeyValueIteratorFacade<K, V>(innerStore.All());

        public V Fetch(K key, long time)
        {
            var e = innerStore.Fetch(key, time);
            return e != null ? e.Value : default;
        }

        public IWindowStoreEnumerator<V> Fetch(K key, DateTime from, DateTime to)
            => new WindowStoreIteratorFacade<V>(innerStore.Fetch(key, from, to));

        public IKeyValueEnumerator<Windowed<K>, V> FetchAll(DateTime from, DateTime to)
            => new KeyValueIteratorFacade<K, V>(innerStore.FetchAll(from, to));
    }
}
