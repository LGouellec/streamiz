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

            // if the value is null, we do not need to forward its selected key-value further
            // if the selected repartition key or value is null, skip
            // forward oldPair first, to be consistent with reduce and aggregate
            if (value.OldValue != null)
            {
                KeyValuePair<K1, V1> oldPair = mapper.Apply(key, value.OldValue);
                if (oldPair.Key != null && oldPair.Value != null)
                    this.Forward(oldPair.Key, new Change<V1>(oldPair.Value, default));
            }

            if (value.NewValue != null)
            {
                KeyValuePair<K1, V1> newPair = mapper.Apply(key, value.NewValue);
                if (newPair.Key != null && newPair.Value != null)
                    this.Forward(newPair.Key, new Change<V1>(default, newPair.Value));
            }
        }
    }
}
