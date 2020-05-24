using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table.Internal;
using System;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KTableReduceProcessor<K, V> : AbstractKTableProcessor<K, V, K, V>
    {
        private readonly Reducer<V> adder;
        private readonly Reducer<V> substractor;


        public KTableReduceProcessor(string storeName, bool sendOldValues, Reducer<V> adder, Reducer<V> substractor)
            : base(storeName, sendOldValues)
        {
            this.adder = adder;
            this.substractor = substractor;
        }

        public override void Process(K key, Change<V> value)
        {
            // the keys should never be null
            if (key == null)
            {
                throw new StreamsException($"Record key for KTable reduce operator with state {queryableStoreName} should not be null.");
            }

            ValueAndTimestamp<V> oldAggAndTimestamp = store.Get(key);
            V oldAgg = oldAggAndTimestamp != null ? oldAggAndTimestamp.Value : default;
            V intermediateAgg;
            long newTimestamp;

            // first try to remove the old value
            if (value.OldValue != null && oldAgg != null)
            {
                intermediateAgg = substractor.Apply(oldAgg, value.OldValue);
                newTimestamp = Math.Max(Context.Timestamp, oldAggAndTimestamp.Timestamp);
            }
            else
            {
                intermediateAgg = oldAgg;
                newTimestamp = Context.Timestamp;
            }

            // then try to add the new value
            V newAgg;
            if (value.NewValue != null)
            {
                if (intermediateAgg == null)
                {
                    newAgg = value.NewValue;
                }
                else
                {
                    newAgg = adder.Apply(intermediateAgg, value.NewValue);
                    newTimestamp = Math.Max(Context.Timestamp, oldAggAndTimestamp.Timestamp);
                }
            }
            else
            {
                newAgg = intermediateAgg;
            }

            // update the store with the new value
            store.Put(key, ValueAndTimestamp<V>.Make(newAgg, newTimestamp));
            tupleForwarder.MaybeForward(key, newAgg, sendOldValues ? oldAgg : default, newTimestamp);
        }
    }
}
