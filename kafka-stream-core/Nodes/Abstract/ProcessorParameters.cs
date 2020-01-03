using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Nodes
{
    internal class ProcessorParameters<T, K, V> where T : NodeParameter<K,V>
    {
        public string Name { get; }
        public T Param { get; }

        public ProcessorParameters(T @param, string name)
        {
            Param = param;
            Name = name;
        }
    }
}
