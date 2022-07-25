using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Mock.Pipes
{
    internal class KafkaPipeInput : IPipeInput
    {
        private readonly string topicName;
        private readonly IProducer<byte[], byte[]> producer;
        private const int size = 10;
        private readonly Queue<(byte[], byte[], DateTime, Headers)> buffer = new Queue<(byte[], byte[], DateTime, Headers)>(size);

        public KafkaPipeInput(string topicName, IStreamConfig configuration, IKafkaSupplier kafkaSupplier)
        {
            this.topicName = topicName;
            producer = kafkaSupplier.GetProducer(
                configuration.ToProducerConfig($"pipe-input-{configuration.ApplicationId}-{topicName}"));
        }

        public string TopicName => topicName;
        public event PipeFlushed Flushed;

        public void Dispose()
        {
            if (buffer.Count > 0)
                Flush();

            producer.Dispose();
        }

        public void Flush()
        {
            while (buffer.Count > 0)
            {
                var record = buffer.Dequeue();
                producer.Produce(topicName,
                    new Message<byte[], byte[]> { Key = record.Item1, Value = record.Item2, Timestamp = new Timestamp(record.Item3), Headers = record.Item4 });
            }
            producer.Flush();
            Flushed?.Invoke();
        }

        public void Pipe(byte[] key, byte[] value, DateTime timestamp, Headers headers)
        {
            buffer.Enqueue((key, value, timestamp, headers));
            if (buffer.Count >= size)
                Flush();
        }
    }
}
