using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.State.Metered
{
    internal class MeteredTimestampedWindowStore<K, V> 
        : MeteredWindowStore<K, ValueAndTimestamp<V>>, ITimestampedWindowStore<K, V>
    {
        public MeteredTimestampedWindowStore(
            IWindowStore<Bytes, byte[]> wrapped,
            long windowSizeMs,
            ISerDes<K> keySerdes,
            ISerDes<ValueAndTimestamp<V>> valueSerdes,
            string metricScope)
            : base(wrapped, windowSizeMs, keySerdes, valueSerdes, metricScope)
        { }

        public override void InitStoreSerde(ProcessorContext context)
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