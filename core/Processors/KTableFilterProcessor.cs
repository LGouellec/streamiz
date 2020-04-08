using kafka_stream_core.State;
using kafka_stream_core.Table.Internal;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors
{
    internal class KTableFilterProcessor<K, V> : AbstractKTableProcessor<K, V, K, V>
    {
        private Func<K, V, bool> predicate;
        private bool filterNot;

        public KTableFilterProcessor(Func<K, V, bool> predicate, bool filterNot, string queryableStoreName, bool sendOldValues)
            : base(queryableStoreName, sendOldValues)
        {
            this.predicate = predicate;
            this.filterNot = filterNot;
        }

        public override object Clone()
        {
            var p = new KTableFilterProcessor<K, V>(this.predicate, this.filterNot, this.queryableStoreName, this.sendOldValues);
            p.StateStores = new List<string>(this.StateStores);
            return p;
        }

        public override void Process(K key, Change<V> change)
        {
            LogProcessingKeyValue(key, change);
            V newValue = ComputeValue(key, change.NewValue);
            V oldValue = sendOldValues ? ComputeValue(key, change.OldValue) : default(V);

            if (sendOldValues && oldValue == null && newValue == null)
            {
                return; // unnecessary to forward here.
            }

            if (queryableStoreName != null)
            {
                store.Put(key, ValueAndTimestamp<V>.Make(newValue, Context.Timestamp));
                //TODO : tupleForwarder.maybeForward(key, newValue, oldValue);
            }
            else
            {
                this.Forward(key, new Change<V>(oldValue, newValue));
            }
        }

        private V ComputeValue(K key, V value)
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
