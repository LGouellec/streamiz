using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table.Internal;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KTableMapValuesProcessor<K, V, VR> : AbstractKTableProcessor<K, V, K, VR>
    {
        private readonly IValueMapperWithKey<K, V, VR> mapper;

        public KTableMapValuesProcessor(IValueMapperWithKey<K, V, VR> mapper, bool sendOldValues, string queryableStoreName)
            : base(queryableStoreName, sendOldValues)
        {
            this.mapper = mapper;
        }

        public override void Process(K key, Change<V> change)
        {
            LogProcessingKeyValue(key, change);
            VR newValue = ComputeValue(key, change.NewValue);
            VR oldValue = this.sendOldValues ? ComputeValue(key, change.OldValue) : default(VR);

            if (this.queryableStoreName != null)
            {
                store.Put(key, ValueAndTimestamp<VR>.Make(newValue, Context.Timestamp));
                tupleForwarder.MaybeForward(key, newValue, oldValue);
            }
            else
            {
                this.Forward(key, new Change<VR>(oldValue, newValue));
            }
        }

        private VR ComputeValue(K key, V value)
        {
            VR newValue = default(VR);

            if (value != null)
            {
                newValue = mapper.Apply(key, value);
            }

            return newValue;
        }
    }
}
