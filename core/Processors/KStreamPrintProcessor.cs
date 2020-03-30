using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors
{
    internal class KStreamPrintProcessor<K, V> : AbstractProcessor<K, V>
    {
        public override object Clone()
        {
            var p= new KStreamPrintProcessor<K, V>();
            p.StateStores = new List<string>(this.StateStores);
            return p;
        }

        public override void Process(K key, V value)
        {
            // TODO : 
        }
    }
}
