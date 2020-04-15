using System;
using System.Collections.Generic;
using System.Text;
using Kafka.Streams.Net.Stream.Internal.Graph;

namespace Kafka.Streams.Net.Processors
{
    internal class KStreamPrintProcessor<K, V> : AbstractProcessor<K, V>
    {
        private PrintForeachAction<K, V> actionPrint;

        public KStreamPrintProcessor(PrintForeachAction<K, V> actionPrint)
        {
            this.actionPrint = actionPrint;
        }

        public override object Clone()
        {
            var p= new KStreamPrintProcessor<K, V>(this.actionPrint);
            p.StateStores = new List<string>(this.StateStores);
            return p;
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            actionPrint.Apply(key, value);
        }

        public override void Close()
        {
            base.Close();
            actionPrint.Close();
        }
    }
}
