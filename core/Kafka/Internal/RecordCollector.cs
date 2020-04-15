using Confluent.Kafka;
using Kafka.Streams.Net.Crosscutting;
using Kafka.Streams.Net.Processors;
using Kafka.Streams.Net.SerDes;
using log4net;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Net.Kafka.Internal
{
    internal class RecordCollector : IRecordCollector
    {
        // TODO : log
        private IProducer<byte[], byte[]> producer;
        private readonly IDictionary<TopicPartition, long> offsets;
        private readonly string logPrefix;
        private readonly ILog log = Logger.GetLogger(typeof(RecordCollector));

        public RecordCollector(string logPrefix)
        {
            this.logPrefix = $"{logPrefix}";
        }

        public void Init(IProducer<byte[], byte[]> producer)
        {
            this.producer = producer;
        }

        public void Close()
        {
            // TODO :
        }

        public void Flush()
        {
            // TODO :
        }

        public void Offsets()
        {
            // TODO :
        }

        public void Send<K, V>(string topic, K key, V value, Headers headers, int partition, long timestamp, ISerDes<K> keySerializer, ISerDes<V> valueSerializer)
        {
            // TODO :
        }

        public void Send<K, V>(string topic, K key, V value, Headers headers, long timestamp, ISerDes<K> keySerializer, ISerDes<V> valueSerializer, IStreamPartitioner<K, V> partitioner)
        {
            // TODO :
            var k = key != null ? keySerializer.Serialize(key) : null;
            var v = value != null ? valueSerializer.Serialize(value) : null;
            producer.Produce(topic, new Message<byte[], byte[]> { Key = k, Value = v });
        }
    }
}
