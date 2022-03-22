using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.State.Metered
{
    internal class MeteredTimestampedKeyValueStore<K, V>
        : MeteredKeyValueStore<K, ValueAndTimestamp<V>>, ITimestampedKeyValueStore<K, V>
    {
        private bool initStoreSerdes = false;

        public MeteredTimestampedKeyValueStore(
            IKeyValueStore<Bytes, byte[]> wrapped,
            ISerDes<K> keySerdes,
            ISerDes<ValueAndTimestamp<V>> valueSerdes,
            string metricScope) 
            : base(wrapped, keySerdes, valueSerdes, metricScope)
        { }
        
        public override void InitStoreSerDes(ProcessorContext context)
        {
            if (!initStoreSerdes)
            {
                keySerdes = keySerdes == null ? context.Configuration.DefaultKeySerDes as ISerDes<K> : keySerdes;
                valueSerdes = valueSerdes == null ? new ValueAndTimestampSerDes<V>(context.Configuration.DefaultValueSerDes as ISerDes<V>) : valueSerdes;
                initStoreSerdes = true;
            }
        }
    }
}