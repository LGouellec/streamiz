using Streamiz.Kafka.Net.State.Enumerator;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class ReadOnlyKeyValueStoreFacade<K, V> : IReadOnlyKeyValueStore<K, V>
    {
        private readonly ITimestampedKeyValueStore<K, V> innerStore;

        public ReadOnlyKeyValueStoreFacade(ITimestampedKeyValueStore<K, V> store)
        {
            innerStore = store;
        }

        public IEnumerable<KeyValuePair<K, V>> All()
            => innerStore
                .All()
                .Select(i => new KeyValuePair<K, V>(i.Key, i.Value != null ? i.Value.Value : default))
                .ToList();

        public long ApproximateNumEntries() => innerStore.ApproximateNumEntries();

        public V Get(K key) => innerStore.Get(key) != null ? innerStore.Get(key).Value : default;

        public IKeyValueEnumerator<K, V> Range(K from, K to)
            => innerStore.Range(from, to).Transform((k, v) => new KeyValuePair<K, V>(k, v.Value));

        public IEnumerable<KeyValuePair<K, V>> ReverseAll()
            => innerStore
                .ReverseAll()
                .Select(i => new KeyValuePair<K, V>(i.Key, i.Value != null ? i.Value.Value : default))
                .ToList();

        public IKeyValueEnumerator<K, V> ReverseRange(K from, K to)
            => innerStore.ReverseRange(from, to).Transform((k, v) => new KeyValuePair<K, V>(k, v.Value));
    }
}