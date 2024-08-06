using System;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamPeekProcessor<K, V> : AbstractProcessor<K, V>
    {
        private readonly Action<K, V, IRecordContext> action;
        private readonly bool forwardDownStream;

        public KStreamPeekProcessor(Action<K, V, IRecordContext> action, bool forwardDownStream)
        {
            this.action = action;
            this.forwardDownStream = forwardDownStream;
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            action.Invoke(key, value, Context.RecordContext);
            if (forwardDownStream)
                Forward(key, value);
        }
    }
}
