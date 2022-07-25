using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.State.Metered
{
    internal class MeteredTimestampedKeyValueStore<K, V>
        : MeteredKeyValueStore<K, ValueAndTimestamp<V>>, ITimestampedKeyValueStore<K, V>
    {
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
                keySerdes ??= context.Configuration.DefaultKeySerDes as ISerDes<K>;
                valueSerdes ??= new ValueAndTimestampSerDes<V>(context.Configuration.DefaultValueSerDes as ISerDes<V>);
                
                keySerdes?.Initialize(new SerDesContext(context.Configuration));
                valueSerdes?.Initialize(new SerDesContext(context.Configuration));
                
                initStoreSerdes = true;
            }
        }
    }
}