using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class StatefullProcessor<K, V, K1, V1> : AbstractProcessor<K, V>
    {
        protected readonly string storeName;
        protected readonly bool sendOldValues;
        protected TimestampedKeyValueStore<K1, V1> store;
        protected TimestampedTupleForwarder<K, V> tupleForwarder;

        protected StatefullProcessor(string storeName, bool sendOldValues)
        {
            this.storeName = storeName;
            this.sendOldValues = sendOldValues;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            store = (TimestampedKeyValueStore<K1, V1>)context.GetStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<K, V>(this, sendOldValues);
        }
    }
}
