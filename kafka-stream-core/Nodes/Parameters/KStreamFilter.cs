using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Nodes
{
    internal class KStreamFilter<K,V> : NodeParameter<K,V>
    {

        public Func<K, V, bool> Predicate { get; }

        public KStreamFilter(Func<K, V, bool> predicate)
        {
            Predicate = predicate;
        }
    }
}
