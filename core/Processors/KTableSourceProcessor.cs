using System;
using System.Collections.Generic;
using System.Text;
using kafka_stream_core.Stream.Internal.Graph;
using kafka_stream_core.Table.Internal.Graph;

namespace kafka_stream_core.Processors
{
    internal class KTableSourceProcessor<K, V> : AbstractProcessor<K, V>
    {
        private KTableSource<K, V> tableSource;

        public KTableSourceProcessor(KTableSource<K, V> tableSource)
        {
            this.tableSource = tableSource;
        }

        public override void Process(K key, V value)
        {
            throw new NotImplementedException();
        }
    }
}
