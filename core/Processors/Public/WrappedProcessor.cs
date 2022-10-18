using System;

namespace Streamiz.Kafka.Net.Processors.Public
{
    internal class WrappedProcessor<K, V> : IProcessor<K, V>
    {
        private readonly Action<Record<K, V>> intern;

        public WrappedProcessor(Action<Record<K, V>> intern)
        {
            this.intern = intern;
        }

        public void Init(ProcessorContext context)
        { }

        public void Process(Record<K, V> record)
            => intern.Invoke(record);

        public void Close()
        { }
    }
}