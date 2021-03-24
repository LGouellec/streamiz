using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// Provides access to the <see cref="IQueryableStoreType{T, K, V}"/>s provided with <see cref="KafkaStream"/>.
    /// These can be used with <see cref="KafkaStream.Store{T, K, V}(StoreQueryParameters{T, K, V})"/>
    /// to access and query the <see cref="IStateStore"/>s that are part of a <see cref="Topology"/>.
    /// </summary>
    public class QueryableStoreTypes
    {
        /// <summary>
        /// A <see cref="IQueryableStoreType{T, K, V}"/> that accepts <see cref="IReadOnlyKeyValueStore{K, V}"/> as T.
        /// </summary>
        /// <typeparam name="K">key type of the store</typeparam>
        /// <typeparam name="V">value type of the store</typeparam>
        /// <returns><see cref="KeyValueStore{K, V}"/></returns>
        public static IQueryableStoreType<IReadOnlyKeyValueStore<K, V>, K, V> KeyValueStore<K, V>() => new KeyValueStoreType<K, V>();

        /// <summary>
        /// A <see cref="IQueryableStoreType{T, K, V}"/> that accepts <see cref="IReadOnlyKeyValueStore{K, U}"/> as T where
        /// U is <see cref="ValueAndTimestamp{V}"/>.
        /// </summary>
        /// <typeparam name="K">key type of the store</typeparam>
        /// <typeparam name="V">value type of the store</typeparam>
        /// <returns><see cref="QueryableStoreTypes.TimestampedKeyValueStore{K, V}"/></returns>
        public static IQueryableStoreType<IReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>, K, ValueAndTimestamp<V>> TimestampedKeyValueStore<K, V>() => new TimestampedKeyValueStoreType<K, V>();

        /// <summary>
        /// A <see cref="IQueryableStoreType{T, K, V}"/> that accepts <see cref="IReadOnlyWindowStore{K, V}"/>.
        /// </summary>
        /// <typeparam name="K">key type of the store</typeparam>
        /// <typeparam name="V">value type of the store</typeparam>
        /// <returns><see cref="QueryableStoreTypes.WindowStore{K, V}"/></returns>
        public static IQueryableStoreType<IReadOnlyWindowStore<K, V>, K, V> WindowStore<K, V>() => new WindowStoreType<K, V>();
    }

    internal class KeyValueStoreType<K, V> : QueryableStoreTypeMatcher<IReadOnlyKeyValueStore<K, V>, K, V>
    {
        public KeyValueStoreType() 
            : base(new[] { typeof(IReadOnlyKeyValueStore<K, V>), typeof(IReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>) })
        {
        }

        public override IReadOnlyKeyValueStore<K, V> Create(IStateStoreProvider<IReadOnlyKeyValueStore<K, V>, K, V> storeProvider, string storeName)
        {
            return new CompositeReadOnlyKeyValueStore<K, V>(storeProvider, this, storeName);
        }
    }

    internal class TimestampedKeyValueStoreType<K, V>: QueryableStoreTypeMatcher<IReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>, K, ValueAndTimestamp<V>>
    {
        public TimestampedKeyValueStoreType(): base(new[] { typeof(IReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>), typeof(ITimestampedKeyValueStore<K, V>) })
        {
        }

        public override IReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> Create(IStateStoreProvider<IReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>, K, ValueAndTimestamp<V>> storeProvider, string storeName)
        {
            return new CompositeReadOnlyKeyValueStore<K, ValueAndTimestamp<V>>(storeProvider, this, storeName);
        }
    }

    internal class WindowStoreType<K, V> : QueryableStoreTypeMatcher<IReadOnlyWindowStore<K, V>, K, V>
    {
        public WindowStoreType()
        : base(new[] { typeof(IReadOnlyWindowStore<K, V>), typeof(ITimestampedWindowStore<K, V>) })
        {
        }

        public override IReadOnlyWindowStore<K, V> Create(IStateStoreProvider<IReadOnlyWindowStore<K, V>, K, V> storeProvider, string storeName)
        {
            return new CompositeReadOnlyWindowStore<K, V>(storeProvider, this, storeName);
        }
    }

    internal abstract class QueryableStoreTypeMatcher<T, K, V> : IQueryableStoreType<T, K, V> where T : class
    {

        private readonly IEnumerable<Type> matchTo;

        protected QueryableStoreTypeMatcher(IEnumerable<Type> matchTo)
        {
            this.matchTo = matchTo;
        }

        public abstract T Create(IStateStoreProvider<T, K, V> storeProvider, string storeName);

        public bool Accepts(IStateStore stateStore)
        {
            return this.matchTo.Any(type => type.IsAssignableFrom(stateStore.GetType()));
        }
    }
}