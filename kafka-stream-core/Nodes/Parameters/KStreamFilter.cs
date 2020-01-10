using System;

namespace kafka_stream_core.Nodes.Parameters
{
    internal class KStreamFilter<K,V> : NodeParameter<K,V>
    {

        public Func<K, V, bool> Predicate { get; }
        public bool Not { get; }

        public KStreamFilter(Func<K, V, bool> predicate, bool not=false)
        {
            Predicate = predicate;
            Not = not;
        }
    }
}
