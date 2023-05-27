using Confluent.Kafka;
using System;
using Streamiz.Kafka.Net.Mock.Sync;

namespace Streamiz.Kafka.Net.Mock.Pipes
{
    internal class SyncPipeInput : IPipeInput
    {
        private readonly ISyncPublisher publisher;
        private readonly string topic;

        public SyncPipeInput(ISyncPublisher publisher, string topic)
        {
            this.publisher = publisher;
            this.topic = topic;
        }

        public string TopicName => topic;
        public event PipeFlushed Flushed;

        public void Dispose()
        {
            Flush();
            publisher.Close();
        }

        public void Flush()
        {
            publisher.Flush();
            Flushed?.Invoke();
        }

        public void Pipe(byte[] key, byte[] value, DateTime timestamp, Headers headers)
        {
            publisher.PublishRecord(topic, key, value, timestamp, headers);
        }
    }
}
