using Streamiz.Kafka.Net.Processors.Public;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamProcessor<K, V> : KStreamPAPI<K, V>
    {
        private readonly Streamiz.Kafka.Net.Processors.Public.IProcessor<K, V> processor;

        public KStreamProcessor(ProcessorSupplier<K, V> processorSupplier)
        {
            processor = processorSupplier.Processor;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            var newContext = new ProcessorContext<K, V>(context);
            processor.Init(newContext);
        }

        public override void Close()
        {
            base.Close();
            processor.Close();
        }

        public override void Process(Record<K, V> record)
            => processor.Process(record);
    }
}