using System;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Processors.Public
{
    public class ProcessorBuilder
    {
        public static ProcessorBuilder<K, V> New<K, V>()
            => new();
    }
    
    public class ProcessorBuilder<K, V>
    {
        private ProcessorSupplier<K, V> processorSupplier = new();

        public ProcessorBuilder<K, V> Processor(IProcessor<K, V> processor)
        {
            processorSupplier.Processor = processor;
            return this;
        }
        
        public ProcessorBuilder<K, V> Processor(Action<Record<K, V>> processor)
        {
            processorSupplier.Processor = new WrappedProcessor<K, V>(processor);
            return this;
        }

        public ProcessorBuilder<K, V> StateStore(StoreBuilder storeBuilder)
        {
            processorSupplier.StoreBuilder = storeBuilder;
            return this;
        }

        public ProcessorSupplier<K, V> Build()
        {
            ProcessorSupplier<K, V> processor = processorSupplier;
            processorSupplier = new();
            return processor;
        }
    }
}