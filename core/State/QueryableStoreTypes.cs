using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Internal;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State
{
    public class QueryableStoreTypes
    {
        public static IQueryableStoreType<ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>> TimestampedKeyValueStore<K, V>()
        {
            return new TimestampedKeyValueStoreType<K, V>();
        }
    }

    internal class TimestampedKeyValueStoreType<K, V>: QueryableStoreTypeMatcher<ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>>
    {
        public TimestampedKeyValueStoreType(): base(new[] { typeof(ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>), typeof(TimestampedKeyValueStore<K, V>) })
        {
        }

        public override ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> Create(IStateStoreProvider storeProvider, string storeName)
        {
            return new CompositeReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>(storeProvider, this, storeName);
        }
    }

    internal abstract class QueryableStoreTypeMatcher<T> : IQueryableStoreType<T>
    {

        private IEnumerable<Type> matchTo;

        public QueryableStoreTypeMatcher(IEnumerable<Type> matchTo)
        {
            this.matchTo = matchTo;
        }

        public abstract T Create(IStateStoreProvider storeProvider, string storeName);

        public bool Accepts(IStateStore stateStore)
        {
            return this.matchTo.All(type => type.IsAssignableFrom(stateStore.GetType()));
        }
        
    }

}
