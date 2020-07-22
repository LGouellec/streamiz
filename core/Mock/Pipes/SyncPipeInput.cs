using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Mock.Pipes
{
    internal class SyncPipeInput : IPipeInput
    {
        private readonly StreamTask task;
        private readonly string topic;

        public SyncPipeInput(StreamTask task, string topic)
        {
            this.task = task;
            this.topic = topic;
        }

        public string TopicName => topic;

        public void Dispose()
        {
            Flush();
        }

        public void Flush()
        {
            while (task.CanProcess)
                task.Process();
        }

        public void Pipe(byte[] key, byte[] value, DateTime timestamp)
        {
            task.AddRecords(new List<ConsumeResult<byte[], byte[]>> {
                    new ConsumeResult<byte[], byte[]> {
                        Topic = topic,
                        TopicPartitionOffset = new TopicPartitionOffset(new TopicPartition(topic, task.Id.Partition), 0),
                        Message = new Message<byte[], byte[]> { Key = key, Value = value, Timestamp = new Timestamp(timestamp) } } });
        }
    }
}
