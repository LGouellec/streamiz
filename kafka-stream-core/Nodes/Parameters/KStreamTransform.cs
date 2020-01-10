using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Nodes.Parameters
{
    internal class KStreamTransform<K, V, K1, V1> : NodeParameter<K, V>
    {
        public Func<K, V, KeyValuePair<K1,V1>> Transform { get; }

        public KStreamTransform(Func<K, V, KeyValuePair<K1, V1>> transform)
        {
            Transform = transform;
        }
    }
}
