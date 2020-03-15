using kafka_stream_core.Stream.Internal.Graph.Nodes;
using System;

namespace kafka_stream_core.Processors
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

        public override void Process(K key, V value)
        {
            if ((!not && predicate.Invoke(key, value)) || (not && !predicate.Invoke(key, value)))
                this.Forward(key, value);
        }
    }
}
