using Kafka.Streams.Net.Crosscutting;
using Kafka.Streams.Net.State;
using Kafka.Streams.Net.Table.Internal;
using log4net;
using System.Collections.Generic;

namespace Kafka.Streams.Net.Processors
{
    internal class KTableSourceProcessor<K, V> : AbstractProcessor<K, V>
    {
        private string storeName;
        private string queryableName;
        private bool sendOldValues;

        private TimestampedKeyValueStore<K, V> store;


        public KTableSourceProcessor(string storeName, string queryableName, bool sendOldValues)
        {
            this.storeName = storeName;
            this.queryableName = queryableName;
            this.sendOldValues = sendOldValues;
        }

        public override object Clone()
        {
            var p = new KTableSourceProcessor<K, V>(this.storeName, this.queryableName, this.sendOldValues);
            p.StateStores = new List<string>(this.StateStores);
            return p;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);

            if (this.queryableName != null)
            {
                store = (TimestampedKeyValueStore<K, V>)context.GetStateStore(queryableName);
                // TODO : 
                //tupleForwarder = new TimestampedTupleForwarder<>(
                //    store,
                //    context,
                //    new TimestampedCacheFlushListener<>(context),
                //    sendOldValues);
            }
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            if (key == null)
            {
                log.Warn($"Skipping record due to null key. topic=[{Context.Topic}] partition=[{Context.Partition}] offset=[{Context.Offset}]");
                return;
            }

            if (queryableName != null)
            {
                 ValueAndTimestamp<V> oldValueAndTimestamp = store.Get(key);
                 V oldValue;
                if (oldValueAndTimestamp != null)
                {
                    oldValue = oldValueAndTimestamp.Value;
                    if (Context.Timestamp < oldValueAndTimestamp.Timestamp)
                    {
                        log.Warn($"Detected out-of-order KTable update for {store.Name} at offset {Context.Offset}, partition {Context.Partition}.");
                    }
                }
                else
                {
                    oldValue = default(V);
                }
                store.Put(key, ValueAndTimestamp<V>.Make(value, Context.Timestamp));
                //tupleForwarder.maybeForward(key, value, oldValue);
            }
            else
            {
                this.Forward<K, Change<V>>(key, new Change<V>(default(V), value));
            }
        }
    }
}
