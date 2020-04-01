using Confluent.Kafka;
using kafka_stream_core.Processors;
using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;

namespace kafka_stream_core.Kafka.Internal
{
    internal class RecordCollectorImpl : IRecordCollector
    {
        private IProducer<byte[], byte[]> producer;
        private readonly IDictionary<TopicPartition, long> offsets;
        private readonly string logPrefix;

        public RecordCollectorImpl(string streamTaskId)
        {
            this.logPrefix = $"task [{streamTaskId}] ";
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
