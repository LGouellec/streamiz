using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Public;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamProcessor<K, V> : AbstractProcessor<K, V>
    {
        private readonly ProcessorSupplier<K, V> processorSupplier;

        public KStreamProcessor(ProcessorSupplier<K, V> processorSupplier)
        {
            this.processorSupplier = processorSupplier;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            processorSupplier.Processor.Init(context);
        }

        public override void Close()
        {
            base.Close();
            processorSupplier.Processor.Close();
        }

        public override void Process(K key, V value)
        {
            Record<K, V> record = new Record<K, V>(
                new TopicPartitionOffset(Context.Topic, Context.Partition, Context.Offset),
                Context.RecordContext.Headers,
                new Timestamp(Context.RecordContext.Timestamp.FromMilliseconds()),
                key,
                value);
            
            processorSupplier.Processor.Process(record);
        }
    }
}