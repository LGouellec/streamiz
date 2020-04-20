using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    internal class PassThroughProcessor<K, V> : AbstractProcessor<K, V>
    {
        public override object Clone()
        {
            var p = new PassThroughProcessor<K, V>();
            p.StateStores = new List<string>(this.StateStores);
            return p;
        }

        public override void Process(K key, V value)
        {
            this.Forward(key, value);
        }
    }
}
