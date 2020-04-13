using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kafka_stream_core.Mock.Kafka
{
    internal class MockTopic
    {
        private string topic;
        private int partitionNumber;
        private readonly MockPartition[] partitions;

        public MockTopic(string topic, int part)
        {
            this.topic = topic;
            this.partitionNumber = part;

            this.partitions = new MockPartition[this.partitionNumber];
        }

        public string Name => topic;

        public void AddMessage(byte[] key, byte[] value, int partition)
        {
            partitions[partition].AddMessageInLog(key, value);
        }

        public (byte[], byte[]) GetMessage(int consumerOffset)
        {
            for (int i = 0; i < this.partitionNumber; ++i)
            {
                if (this.partitions[i].Size > consumerOffset)
                {
                    return this.partitions[i].GetMessage(consumerOffset);
                }
            }
            return default;
        }
    }
}
