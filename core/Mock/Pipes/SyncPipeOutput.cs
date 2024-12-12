using Confluent.Kafka;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock.Sync;
using System;
using System.Collections.Generic;
using System.Threading;
using Streamiz.Kafka.Net.Kafka.Internal;

namespace Streamiz.Kafka.Net.Mock.Pipes
{
    internal class SyncPipeOutput : IPipeOutput
    {
        private readonly string topicName;
        private readonly TimeSpan timeout;
        private readonly CancellationToken token;
        private readonly IConsumer<byte[], byte[]> consumer;


        public SyncPipeOutput(string topic, TimeSpan consumeTimeout, IStreamConfig configuration, SyncProducer producer, CancellationToken token)
        {
            this.token = token;
            topicName = topic;
            timeout = consumeTimeout;
            consumer = new SyncConsumer(producer);
            (consumer as SyncConsumer).UseConfig(configuration.ToConsumerConfig($"pipe-output-{configuration.ApplicationId}-{topicName}"));
            consumer.Subscribe(topicName);
        }

        public string TopicName => topicName;

        public int Size => throw new InvalidOperationException("Operation not available in synchronous mode");

        public bool IsEmpty => throw new InvalidOperationException("Operation not available in synchronous mode");

        public List<PipeOutputInfo> GetInfos() => throw new InvalidOperationException("Operation not available in synchronous mode");

        public void Dispose()
        {
            consumer.Unsubscribe();
            consumer.Dispose();
        }

        public ConsumeResult<byte[], byte[]> Read()
        {
            int count = 0;
            while (count <= 10 && !token.IsCancellationRequested)
            {
                var record = consumer.Consume(timeout);
                if (record != null)
                {
                    consumer.Commit(record);
                    return record;
                }
                else
                {
                    Thread.Sleep((int)timeout.TotalMilliseconds / 10);
                    ++count;
                }
            }

            throw new StreamsException($"No record found in topic {topicName} after {timeout.TotalSeconds}s !");
        }

        public IEnumerable<ConsumeResult<byte[], byte[]>> ReadList()
        {
            List<ConsumeResult<byte[], byte[]>> records = new List<ConsumeResult<byte[], byte[]>>();
            ConsumeResult<byte[], byte[]> record = null;
            do
            {
                record = consumer.Consume(timeout);
                if (record != null)
                {
                    consumer.Commit(record);
                    records.Add(record);
                }
            } while (!token.IsCancellationRequested && record != null);
            return records;
        }
    }
}
