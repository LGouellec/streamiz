using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kafka_stream_core.Crosscutting
{
    internal static class KafkaExtensions
    {
        internal static IEnumerable<ConsumeResult<K, V>> ConsumeRecords<K, V>(this IConsumer<K, V> consumer, TimeSpan timeout)
        {
            List<ConsumeResult<K, V>> records = new List<ConsumeResult<K, V>>();
            DateTime dt = DateTime.Now;
            while (dt.Add(timeout) > DateTime.Now)
            {
                var r = consumer.Consume(timeout);
                if (r != null)
                    records.Add(r);
            }
            return records;
        }

        internal static IEnumerable<PartitionMetadata> PartitionsForTopic(this Metadata clusterMetadata, string topic)
        {
            if (clusterMetadata.Topics.Any(t => t.Topic.Equals(topic)))
            {
                return clusterMetadata.Topics.Find(t => t.Topic.Equals(topic)).Partitions;
            }
            else
                return new List<PartitionMetadata>();
        }
    }
}
