using System;
using kafka_stream_core.Stream.Internal.Graph;

namespace kafka_stream_core.Processors
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

        public override void Process(K key, V value)
        {
            this.action.Invoke(key, value);
            if (this.forwardDownStream)
                this.Forward(key, value);
        }
    }
}
