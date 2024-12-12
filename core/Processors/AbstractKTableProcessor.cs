using Confluent.Kafka;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class AbstractKTableProcessor<K, V, KS, VS> : AbstractProcessor<K, Change<V>>
    {
        protected readonly string queryableStoreName;
        protected readonly bool sendOldValues;
        private readonly bool throwException = false;
        protected ITimestampedKeyValueStore<KS, VS> store;
        protected TimestampedTupleForwarder<KS, VS> tupleForwarder;

        protected AbstractKTableProcessor(string queryableStoreName, bool sendOldValues, bool throwExceptionStateNull = false)
        {
            this.queryableStoreName = queryableStoreName;
            this.sendOldValues = sendOldValues;
            throwException = throwExceptionStateNull;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);

            if (queryableStoreName != null)
            {
                store = (ITimestampedKeyValueStore<KS, VS>)context.GetStateStore(queryableStoreName);
                tupleForwarder = new TimestampedTupleForwarder<KS, VS>(
                    store,
                    this,kv => {
                        context.CurrentProcessor = this;
                        Forward(kv.Key,
                            new Change<VS>(sendOldValues ? kv.Value.OldValue.Value : default, kv.Value.NewValue.Value),
                            kv.Value.NewValue.Timestamp);
                    },
                    sendOldValues);
            }

            if (throwException && (queryableStoreName == null || store == null || tupleForwarder == null))
                throw new StreamsException($"{logPrefix}Processor {Name} doesn't have queryable store name. Please set a correct name to materialed view !");
        }
    }
}
