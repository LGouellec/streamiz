using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class AbstractKTableProcessor<K, V, KS, VS> : AbstractProcessor<K, Change<V>>
    {
        protected readonly string queryableStoreName;
        protected readonly bool sendOldValues;

        protected TimestampedKeyValueStore<KS, VS> store;
        protected TimestampedTupleForwarder<K, V> tupleForwarder;

        public AbstractKTableProcessor(string queryableStoreName, bool sendOldValues)
        {
            this.queryableStoreName = queryableStoreName;
            this.sendOldValues = sendOldValues;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);

            if (queryableStoreName != null)
            {
                store = (TimestampedKeyValueStore<KS, VS>)context.GetStateStore(queryableStoreName);
                tupleForwarder = new TimestampedTupleForwarder<K, V>(this, sendOldValues);
            }
        }
    }
}
