using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors
{
    internal class KStreamPrintProcessor<K, V> : AbstractProcessor<K, V>
    {
        public override void Process(K key, V value)
        {
            // TODO : 
        }
    }
}
