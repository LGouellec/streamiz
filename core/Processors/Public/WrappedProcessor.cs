using System;

namespace Streamiz.Kafka.Net.Processors.Public
{
    internal class WrappedProcessor<K, V> : IProcessor<K, V>, ICloneableProcessor
    {
        private readonly Action<Record<K, V>> intern;

        public WrappedProcessor(Action<Record<K, V>> intern)
        {
            this.intern = intern;
        }

        public void Init(ProcessorContext<K, V> context)
        { }

        public void Process(Record<K, V> record)
            => intern.Invoke(record);

        public void Close()
        { }

        public object Clone()
        {
            return new WrappedProcessor<K, V>(intern);
        }
    }
}