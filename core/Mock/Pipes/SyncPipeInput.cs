using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Mock.Pipes
{
    internal class SyncPipeInput : IPipeInput
    {
        private readonly StreamTask task;

        public SyncPipeInput(StreamTask task)
        {
            this.task = task;
        }

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
            task.AddRecords(task.Partition,
                new List<ConsumeResult<byte[], byte[]>> {
                    new ConsumeResult<byte[], byte[]> {
                        TopicPartitionOffset = new TopicPartitionOffset(task.Partition, 0),
                        Message = new Message<byte[], byte[]> { Key = key, Value = value, Timestamp = new Timestamp(timestamp) } } });
        }
    }
}
