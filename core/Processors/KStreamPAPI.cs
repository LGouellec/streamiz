using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Public;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class KStreamPAPI<K, V> : AbstractProcessor<K, V>
    {
        public abstract void Process(Record<K, V> record);

        public override void Process(K key, V value)
        {
            if (key == null && StateStores.Count > 0)
            {
                log.LogWarning($"Skipping record due to null key because your transformer is stateful. topic=[{Context.Topic}] partition=[{Context.Partition}] offset=[{Context.Offset}]");
                droppedRecordsSensor.Record();
                return;
            }
            
            Record<K, V> record = new Record<K, V>(
                new TopicPartitionOffset(Context.Topic, Context.Partition, Context.Offset),
                Context.RecordContext.Headers,
                new Timestamp(Context.RecordContext.Timestamp.FromMilliseconds()),
                key,
                value);

            Process(record);
        }
    }
}