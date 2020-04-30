using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock.Sync;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Streamiz.Kafka.Net.Mock.Pipes
{
    internal class SyncPipeOutput : IPipeOutput
    {
        private readonly string topicName;
        private readonly TimeSpan timeout;
        private readonly ILog logger = Crosscutting.Logger.GetLogger(typeof(KafkaPipeOutput));
        private readonly CancellationToken token;
        private readonly SyncProducer syncProducer = null;
        private readonly IConsumer<byte[], byte[]> consumer;


        public SyncPipeOutput(string topic, TimeSpan consumeTimeout, IStreamConfig configuration, SyncProducer producer, CancellationToken token)
        {
            this.token = token;
            topicName = topic;
            timeout = consumeTimeout;
            consumer = new SyncConsumer(configuration.ToConsumerConfig($"pipe-output-{configuration.ApplicationId}-{topicName}"), producer);
            consumer.Subscribe(topicName);
        }

        public int Size => throw new InvalidOperationException("Operation not available in synchronous mode");

        public bool IsEmpty => throw new InvalidOperationException("Operation not available in synchronous mode");

        public List<PipeOutputInfo> GetInfos() => throw new InvalidOperationException("Operation not available in synchronous mode");

        public void Dispose()
        {
            consumer.Unsubscribe();
            consumer.Dispose();
        }

        public KeyValuePair<byte[], byte[]> Read()
        {
            int count = 0;
            while (count <= 10 && !token.IsCancellationRequested)
            {
                var record = consumer.Consume(timeout);
                if (record != null)
                {
                    KeyValuePair<byte[], byte[]> kv = KeyValuePair.Create(record.Message.Key, record.Message.Value);
                    consumer.Commit(record);
                    return kv;
                }
                else
                {
                    Thread.Sleep((int)timeout.TotalMilliseconds / 10);
                    ++count;
                }
            }

            throw new StreamsException($"No record found in topic {topicName} after {timeout.TotalSeconds}s !");
        }

        public IEnumerable<KeyValuePair<byte[], byte[]>> ReadList()
        {
            List<KeyValuePair<byte[], byte[]>> records = new List<KeyValuePair<byte[], byte[]>>();
            ConsumeResult<byte[], byte[]> record = null;
            do
            {
                record = consumer.Consume(timeout);
                if (record != null)
                {
                    KeyValuePair<byte[], byte[]> kv = KeyValuePair.Create(record.Message.Key, record.Message.Value);
                    consumer.Commit(record);
                    records.Add(kv);
                }
            } while (!token.IsCancellationRequested && record != null);
            return records;
        }
    }
}
