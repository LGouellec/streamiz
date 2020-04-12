using Confluent.Kafka;
using kafka_stream_core.Errors;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace kafka_stream_core.Mock.Pipes
{
    internal class PipeOutput : IPipeOutput
    {
        private readonly string topicName;
        private readonly TimeSpan timeout;

        private readonly CancellationToken token;
        private readonly Thread readThread;
        private readonly IConsumer<byte[], byte[]> consumer;
        private readonly Queue<(byte[], byte[])> queue = new Queue<(byte[], byte[])>();
        private readonly object _lock = new object();

        public PipeOutput(string topicName, TimeSpan timeout, IStreamConfig configuration, CancellationToken token)
        {
            this.topicName = topicName;
            this.timeout = timeout;

            var builder = new ConsumerBuilder<byte[], byte[]>(configuration.ToConsumerConfig($"pipe-output-{configuration.ApplicationId}-{topicName}"));
            this.consumer = builder.Build();

            this.token = token;
            this.readThread = new Thread(ReadThread);
            this.readThread.Start();
        }

        private void ReadThread()
        {
            this.consumer.Subscribe(this.topicName);

            while (!token.IsCancellationRequested)
            {
                var record = consumer.Consume(timeout);
                if (record != null)
                {
                    lock (_lock)
                        queue.Enqueue((record.Key, record.Value));
                    consumer.Commit();
                }
            }
        }

        public void Dispose()
        {
            if (queue.Count > 0) ; // TODO : logger

            consumer.Unsubscribe();
            readThread.Join();
            consumer.Dispose();
        }

        public KeyValuePair<byte[], byte[]> Read()
        {
            int count = 0;
            while (count <= 1)
            {
                int size = 0;
                lock (_lock)
                    size = queue.Count;

                if (size > 0)
                {
                    var record = queue.Dequeue();
                    return new KeyValuePair<byte[], byte[]>(record.Item1, record.Item2);
                }
                else
                {
                    // REFACTOR
                    Thread.Sleep(timeout);
                    ++count;
                }
            }

            throw new StreamsException($"No record found in topic {topicName} after {timeout.TotalSeconds}s !");
        }
    }
}
