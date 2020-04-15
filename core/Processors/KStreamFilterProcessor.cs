using Kafka.Streams.Net.Stream.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Net.Processors
{
    internal class KStreamFilterProcessor<K,V> : AbstractProcessor<K, V>
    {
        private Func<K, V, bool> predicate;
        private bool not;


        public KStreamFilterProcessor(Func<K, V, bool> predicate, bool not)
        {
            this.predicate = predicate;
            this.not = not;
        }

        public override object Clone()
        {
            var p = new KStreamFilterProcessor<K, V>(this.predicate, this.not);
            p.StateStores = new List<string>(this.StateStores);
            return p;
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            if ((!not && predicate.Invoke(key, value)) || (not && !predicate.Invoke(key, value)))
            {
                this.Forward(key, value);
            }
        }
    }
}
