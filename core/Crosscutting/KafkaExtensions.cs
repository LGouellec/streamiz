using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Streamiz.Kafka.Net.Crosscutting
{
    internal static class KafkaExtensions
    {
        internal static List<WatermarkOffsetsByTopicPartition> GetWatermarkOffsets<K, V>(this IConsumer<K, V> consumer)
        {
            List<WatermarkOffsetsByTopicPartition> l = new List<WatermarkOffsetsByTopicPartition>();
            foreach (var p in consumer.Assignment)
            {
                var w = consumer.GetWatermarkOffsets(p);
                l.Add(new WatermarkOffsetsByTopicPartition(p.Topic, p.Partition, w.Low, w.High));
            }
            return l;
        }

        internal static int? PartitionCountForTopic(this Metadata metadata, string topic)
        {
            var topicInfo = metadata.Topics.FirstOrDefault(t => t.Topic.Equals(topic));
            return topicInfo?.Partitions.Count;
        }
        
        // NOT USED FOR MOMENT
        //internal static IEnumerable<PartitionMetadata> PartitionsForTopic(this Metadata clusterMetadata, string topic)
        //{
        //    if (clusterMetadata.Topics.Any(t => t.Topic.Equals(topic)))
        //    {
        //        return clusterMetadata.Topics.Find(t => t.Topic.Equals(topic)).Partitions;
        //    }
        //    else
        //    {
        //        return new List<PartitionMetadata>();
        //    }
        //}

        internal static IEnumerable<ConsumeResult<K, V>> ConsumeRecords<K, V>(this IConsumer<K, V> consumer, TimeSpan timeout)
            => ConsumeRecords(consumer, timeout, 500);

        internal static IEnumerable<ConsumeResult<K, V>> ConsumeRecords<K, V>(this IConsumer<K, V> consumer, TimeSpan timeout, long maxRecords)
        {
            List<ConsumeResult<K, V>> records = new List<ConsumeResult<K, V>>();
            DateTime dt = DateTime.Now, now;
            TimeSpan ts = timeout;
            do
            {
                var r = consumer.Consume(ts);
                now = DateTime.Now;
                if (r != null)
                {
                    records.Add(r);
                }
                else
                {
                    var diffTs = Math.Max((dt.Add(timeout) - now).TotalMilliseconds, 0);
                    ts = TimeSpan.FromMilliseconds(1 / (2 * Math.Log(diffTs) + 1) * 1000);
                    
                    if (consumer.Assignment  != null && !consumer.Assignment.Any()) // if consumer has no assignment, sleep Max (ts / 2, 100 ms)
                        Thread.Sleep(Math.Max((int)ts.TotalMilliseconds / 2, 100));
                    
                    if (ts.TotalMilliseconds == 0) // if not enough time, do not call Consume(0); => break;
                        break;
                }

                if (records.Count >= maxRecords) // if the batch is full, break;
                    break;
                
            } while (dt.Add(timeout) > now);

            return records;
        }

        internal static Headers Clone(this Headers headers)
        {
            if (headers == null) return new Headers();
            
            var originHeader = headers
                .Select(h => (h.Key, h.GetValueBytes()))
                .ToList();

            Headers copyHeaders = new Headers();
            originHeader.ForEach(h => copyHeaders.Add(h.Key, h.Item2));
            return copyHeaders;
        }
        
        internal static Headers AddOrUpdate(this Headers headers, string key, byte[] value)
        {
            if (headers == null)
                headers = new Headers();
            headers.Remove(key);
            headers.Add(key, value);
            return headers;
        }
    }
}
