using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Stream;
using System;

namespace Streamiz.Kafka.Net.Table.Internal.Graph
{
    internal class KTableAggregate<K, V, T> : IKTableProcessorSupplier<K, V, T>
    {
        private readonly string storeName;
        private readonly Initializer<T> initializer;
        private readonly Aggregator<K, V, T> add;
        private readonly Aggregator<K, V, T> remove;

        private bool sendOldValues = false;


        public KTableAggregate(string storeName, Func<T> initializer, Func<K, V, T, T> adder, Func<K, V, T, T> remover)
            : this(storeName, new WrappedInitializer<T>(initializer), new WrappedAggregator<K, V, T>(adder), new WrappedAggregator<K, V, T>(remover))
            { }

        public KTableAggregate(string storeName, Initializer<T> initializer, Aggregator<K, V, T> adder, Aggregator<K, V, T> remover)
        {
            this.storeName = storeName;
            this.initializer = initializer;
            add = adder;
            remove = remover;
        }

        public void EnableSendingOldValues()
        {
            sendOldValues = true;
        }

        public IProcessor<K, Change<V>> Get()
            => new KTableAggregateProcessor<K, V, T>(storeName, sendOldValues, initializer, add, remove);

        public IKTableValueGetterSupplier<K, T> View =>
            new KTableMaterializedValueGetterSupplier<K, T>(storeName);
    }
}
