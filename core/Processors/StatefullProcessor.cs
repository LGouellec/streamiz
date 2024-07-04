using System.Collections.Generic;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class StatefullProcessor<K, V, K1, V1> : AbstractProcessor<K, V>
    {
        protected readonly string storeName;
        protected readonly bool sendOldValues;
        protected ITimestampedKeyValueStore<K1, V1> store;
        protected TimestampedTupleForwarder<K1, V1> tupleForwarder;

        protected StatefullProcessor(string storeName, bool sendOldValues)
        {
            this.storeName = storeName;
            this.sendOldValues = sendOldValues;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            store = (ITimestampedKeyValueStore<K1, V1>)context.GetStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<K1, V1>(
                store,
                this,
                kv =>
                {
                    context.CurrentProcessor = this;
                    Forward(kv.Key,
                        new Change<V1>(sendOldValues ? kv.Value.OldValue.Value : default, kv.Value.NewValue.Value),
                        kv.Value.NewValue.Timestamp);
                },
                sendOldValues);
        }
    }
}
