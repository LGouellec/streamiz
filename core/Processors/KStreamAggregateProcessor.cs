using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using System;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamAggregateProcessor<K, V, T> : AbstractProcessor<K, V>
    {
        private readonly string storeName;
        private readonly bool enableSendOldValues;
        private readonly Initializer<T> initializer;
        private readonly Aggregator<K, V, T> aggregator;

        private TimestampedKeyValueStore<K, T> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;

        public KStreamAggregateProcessor(string storeName, bool enableSendOldValues, Initializer<T> initializer, Aggregator<K, V, T> aggregator)
        {
            this.storeName = storeName;
            this.enableSendOldValues = enableSendOldValues;
            this.initializer = initializer;
            this.aggregator = aggregator;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            store = (TimestampedKeyValueStore<K, T>)context.GetStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<K, V>(this, enableSendOldValues);
        }

        public override void Process(K key, V value)
        {
            if (key == null || value == null)
            {
                log.Warn($"Skipping record due to null key or value. key=[{key}] value=[{value}] topic=[{Context.Topic}] partition=[{Context.Partition}] offset=[{Context.Offset}]");
                return;
            }

            ValueAndTimestamp<T> oldAggAndTimestamp = store.Get(key);
            T oldAgg = oldAggAndTimestamp == null ? default : oldAggAndTimestamp.Value;

             T newAgg;
             long newTimestamp;

            if (oldAgg == null)
            {
                oldAgg = initializer.Apply();
                newTimestamp = Context.Timestamp;
            }
            else
            {
                oldAgg = oldAggAndTimestamp.Value;
                newTimestamp = Math.Max(Context.Timestamp, oldAggAndTimestamp.Timestamp);
            }

            newAgg = aggregator.Apply(key, value, oldAgg);

            store.Put(key, ValueAndTimestamp<T>.Make(newAgg, newTimestamp));
            tupleForwarder.MaybeForward(key, newAgg, enableSendOldValues ? oldAgg : default, newTimestamp);
        }
    }
}