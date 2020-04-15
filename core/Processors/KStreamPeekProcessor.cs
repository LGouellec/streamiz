using System;
using System.Collections.Generic;
using Kafka.Streams.Net.Stream.Internal.Graph;

namespace Kafka.Streams.Net.Processors
{
    internal class KStreamPeekProcessor<K, V> : AbstractProcessor<K, V>
    {
        private Action<K, V> action;
        private bool forwardDownStream;

        public KStreamPeekProcessor(Action<K, V> action, bool forwardDownStream)
        {
            this.action = action;
            this.forwardDownStream = forwardDownStream;
        }

        public override object Clone()
        {
            var p= new KStreamPeekProcessor<K, V>(this.action, this.forwardDownStream);
            p.StateStores = new List<string>(this.StateStores);
            return p;
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            this.action.Invoke(key, value);
            if (this.forwardDownStream)
                this.Forward(key, value);
        }
    }
}
