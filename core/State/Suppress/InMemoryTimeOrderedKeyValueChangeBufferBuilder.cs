using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Suppress.Internal;

namespace Streamiz.Kafka.Net.State.Suppress
{
    internal class InMemoryTimeOrderedKeyValueChangeBufferBuilder<K, V>
        : AbstractStoreBuilder<K, V, InMemoryTimeOrderedKeyValueChangeBuffer<K, V>>
    {
        public InMemoryTimeOrderedKeyValueChangeBufferBuilder(string name, ISerDes<K> keySerde, ISerDes<V> valueSerde) 
            : base(name, keySerde, valueSerde)
        {
            WithCachingDisabled(); // disable explicitly the cache 
        }

        // Not used
        public override bool IsWindowStore => false;
        public override long RetentionMs => -1;
        
        public override InMemoryTimeOrderedKeyValueChangeBuffer<K, V> Build() =>
            new(
                Name,
                LoggingEnabled,
                keySerdes,
                valueSerdes);
    }
}