using kafka_stream_core.State;
using kafka_stream_core.Table.Internal;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors
{
    internal class KTableFilterProcessor<K, V> : AbstractProcessor<K, Change<V>>
    {
        private Func<K, V, bool> predicate;
        private bool filterNot;
        private string queryableStoreName;
        private bool sendOldValues;

        private TimestampedKeyValueStore<K, V> store;
        // TODO : private TimestampedTupleForwarder<K, V> tupleForwarder;

        public KTableFilterProcessor(Func<K, V, bool> predicate, bool filterNot, string queryableStoreName, bool sendOldValues)
        {
            this.predicate = predicate;
            this.filterNot = filterNot;
            this.queryableStoreName = queryableStoreName;
            this.sendOldValues = sendOldValues;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);

            if (queryableStoreName != null)
            {
                store = (TimestampedKeyValueStore<K, V>)context.GetStateStore(queryableStoreName);
                //tupleForwarder = new TimestampedTupleForwarder<>(
                //    store,
                //    context,
                //    new TimestampedCacheFlushListener<>(context),
                //    sendOldValues);
            }
        }

        public override void Process(K key, Change<V> change)
        {
            V newValue = computeValue(key, change.NewValue);
            V oldValue = sendOldValues ? computeValue(key, change.OldValue) : default(V);

            if (sendOldValues && oldValue == null && newValue == null)
            {
                return; // unnecessary to forward here.
            }

            if (queryableStoreName != null)
            {
                store.put(key, ValueAndTimestamp<V>.make(newValue, Context.Timestamp));
                //TODO : tupleForwarder.maybeForward(key, newValue, oldValue);
            }
            else
            {
                this.Forward(key, new Change<V>(oldValue, newValue));
            }
        }

        private V computeValue(K key, V value)
        {
            V newValue = default(V);

            if (value != null && (filterNot ^ predicate.Invoke(key, value)))
            {
                newValue = value;
            }

            return newValue;
        }
    }
}
