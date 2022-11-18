using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Public;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamProcessor<K, V> : AbstractProcessor<K, V>
    {
        private readonly Streamiz.Kafka.Net.Processors.Public.IProcessor<K, V> processor;

        public KStreamProcessor(ProcessorSupplier<K, V> processorSupplier)
        {
            processor = processorSupplier.Processor;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            processor.Init(context);
        }

        public override void Close()
        {
            base.Close();
            processor.Close();
        }

        public override void Process(K key, V value)
        {
            if (key == null && StateStores.Count > 0)
            {
                log.LogWarning($"Skipping record due to null key because your processor is stateful. topic=[{Context.Topic}] partition=[{Context.Partition}] offset=[{Context.Offset}]");
                droppedRecordsSensor.Record();
                return;
            }
            
            Record<K, V> record = new Record<K, V>(
                new TopicPartitionOffset(Context.Topic, Context.Partition, Context.Offset),
                Context.RecordContext.Headers,
                new Timestamp(Context.RecordContext.Timestamp.FromMilliseconds()),
                key,
                value);
            
            processor.Process(record);
        }
    }
}