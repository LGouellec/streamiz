using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table.Internal;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class AbstractKTableProcessor<K, V, KS, VS> : AbstractProcessor<K, Change<V>>
    {
        protected readonly string queryableStoreName;
        protected readonly bool sendOldValues;

        protected TimestampedKeyValueStore<KS, VS> store;
        // TODO : protected TimestampedTupleForwarder<K, V> tupleForwarder;

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
                // TODO :
                //tupleForwarder = new TimestampedTupleForwarder<>(
                //    store,
                //    context,
                //    new TimestampedCacheFlushListener<>(context),
                //    sendOldValues);
            }
        }
    }
}
