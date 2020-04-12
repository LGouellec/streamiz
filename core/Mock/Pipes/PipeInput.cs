﻿using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Mock.Pipes
{
    internal class PipeInput : IPipeInput
    {
        private readonly string topicName;
        private readonly IProducer<byte[], byte[]> producer;
        const int size = 10;
        private readonly Queue<(byte[], byte[], DateTime)> buffer = new Queue<(byte[], byte[], DateTime)>(size);

        public PipeInput(string topicName, IStreamConfig configuration)
        {
            this.topicName = topicName;
            var builder = new ProducerBuilder<byte[], byte[]>(configuration.ToProducerConfig($"pipe-input-{configuration.ApplicationId}-{topicName}"));
            this.producer = builder.Build();
        }

        public void Dispose()
        {
            if (buffer.Count > 0)
                this.Flush();

            this.producer.Dispose();
        }

        public void Flush()
        {
            while(buffer.Count > 0)
            {
                var record = buffer.Dequeue();
                this.producer.Produce(topicName, 
                    new Message<byte[], byte[]> { Key = record.Item1, Value = record.Item2, Timestamp = new Timestamp(record.Item3) }, 
                    (r) =>
                    {
                        int a = 1;
                    });
            }
        }

        public void Pipe(byte[] key, byte[] value, DateTime timestamp)
        {
            buffer.Enqueue((key, value, timestamp));
            if (buffer.Count >= size)
                this.Flush();
        }
    }
}