using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class ReadOnlyKeyValueStoreFacade<K, V> : ReadOnlyKeyValueStore<K, V>
    {
        private readonly TimestampedKeyValueStore<K, V> innerStore;

        public ReadOnlyKeyValueStoreFacade(TimestampedKeyValueStore<K, V> store)
        {
            innerStore = store;
        }

        public IEnumerable<KeyValuePair<K, V>> All()
            => innerStore
                .All()
                .Select(i => KeyValuePair.Create(i.Key, i.Value != null ? i.Value.Value : default))
                .ToList();

        public long ApproximateNumEntries() => innerStore.ApproximateNumEntries();

        public V Get(K key) => innerStore.Get(key) != null ? innerStore.Get(key).Value : default;
    }
}
