using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table.Internal;
using System;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KTableAggregateProcessor<K, V, T> : AbstractKTableProcessor<K, V, K, T>
    {
        private readonly Initializer<T> initializer;
        private readonly Aggregator<K, V, T> add;
        private readonly Aggregator<K, V, T> remove;


        public KTableAggregateProcessor(string storeName, bool sendOldValues, Initializer<T> initializer, Aggregator<K, V, T> add, Aggregator<K, V, T> remove)
            : base(storeName, sendOldValues)
        {
            this.initializer = initializer;
            this.add = add;
            this.remove = remove;
        }

        public override void Process(K key, Change<V> value)
        {
            if (key == null)
            {
                throw new StreamsException($"Record key for KTable aggregate operator with state {queryableStoreName} should not be null.");
            }

            ValueAndTimestamp<T> oldAggAndTimestamp = store.Get(key);
            T oldAgg = oldAggAndTimestamp != null ? oldAggAndTimestamp.Value : default;
            T intermediateAgg;
            long newTimestamp = Context.Timestamp;

            // first try to remove the old value
            if (value.OldValue != null && oldAgg != null)
            {
                intermediateAgg = remove.Apply(key, value.OldValue, oldAgg);
                newTimestamp = Math.Max(Context.Timestamp, oldAggAndTimestamp.Timestamp);
            }
            else
            {
                intermediateAgg = oldAgg;
            }

            // then try to add the new value
            T newAgg;
            if (value.NewValue != null)
            {
                T initializedAgg;
                if (intermediateAgg == null)
                {
                    initializedAgg = initializer.Apply();
                }
                else
                {
                    initializedAgg = intermediateAgg;
                }

                newAgg = add.Apply(key, value.NewValue, initializedAgg);
                if (oldAggAndTimestamp != null)
                {
                    newTimestamp = Math.Max(Context.Timestamp, oldAggAndTimestamp.Timestamp);
                }
            }
            else
            {
                newAgg = intermediateAgg;
            }

            // update the store with the new value
            store.Put(key, ValueAndTimestamp<T>.Make(newAgg, newTimestamp));
            tupleForwarder.MaybeForward(key, newAgg, sendOldValues ? oldAgg : default, newTimestamp);
        }
    }
}