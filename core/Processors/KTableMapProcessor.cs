using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table.Internal;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KTableMapProcessor<K, V, K1, V1> : AbstractKTableProcessor<K, V, K1, V1>
    {
        private readonly IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper;

        public KTableMapProcessor(IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper)
            : base(null, false)
        {
            this.mapper = mapper;
        }

        public override void Process(K key, Change<V> value)
        {
            LogProcessingKeyValue(key, value);
            // the original key should never be null
            if (key == null)
            {
                throw new StreamsException("Record key for the grouping KTable should not be null.");
            }

            KeyValuePair<K1, V1> oldPair = value.OldValue == null ? default : mapper.Apply(key, value.OldValue);
            KeyValuePair<K1, V1> newPair = value.NewValue == null ? default : mapper.Apply(key, value.NewValue);

            bool oldPairNotNull = value.OldValue != null;
            bool newPairNotNull = value.NewValue != null;
            
            // if the selected repartition key or value is null, skip
            // forward oldPair first, to be consistent with reduce and aggregate
            if (oldPairNotNull && newPairNotNull && oldPair.Key.Equals(newPair.Key))
            {
                Forward(oldPair.Key, new Change<V1>(oldPair.Value, newPair.Value));
            }
            else
            {
                if(oldPairNotNull)
                    Forward(oldPair.Key, new Change<V1>(oldPair.Value, default));
                
                if(newPairNotNull)
                    Forward(newPair.Key, new Change<V1>(default, newPair.Value));
            }
        }
    }
}
