using kafka_stream_core.Processors;
using System;

namespace kafka_stream_core.Stream.Internal.Graph.Nodes
{
    internal class ProcessorParameters<K, V>
    {
        public IProcessorSupplier<K, V> Processor { get; private set; }
        public string ProcessorName { get; private set; }

        public ProcessorParameters(IProcessorSupplier<K, V> processorSupplier, String processorName)
        {
            this.Processor = processorSupplier;
            this.ProcessorName = processorName;
        }


        public override string ToString()
        {
            return "ProcessorParameters{" +
                "processor class=" + Processor.Get().GetType() +
                ", processor name='" + ProcessorName + '\'' +
                '}';
        }
    }
}
