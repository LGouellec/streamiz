using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal class MockTopic
    {
        private readonly List<MockPartition> partitions;

        public MockTopic(string topic, int part)
        {
            this.Name = topic;
            this.PartitionNumber = part;

            this.partitions = new List<MockPartition>();
            for (int i = 0; i < this.PartitionNumber; ++i)
                this.partitions.Add(new MockPartition(i));
        }

        public string Name { get; }
        public int PartitionNumber { get; private set; }

        public void AddMessage(byte[] key, byte[] value, int partition)
        {
            partitions[partition].AddMessageInLog(key, value);
        }

        public TestRecord<byte[], byte[]> GetMessage(int partition, long consumerOffset)
        {
            if (this.partitions[partition].Size > consumerOffset)
            {
                return this.partitions[partition].GetMessage(consumerOffset);
            }
            else
                return null;
        }

        internal void CreateNewPartitions(Partition partition)
        {
            var diff = partition - (PartitionNumber - 1);
            for (int i = 0; i < diff; ++i)
                this.partitions.Add(new MockPartition(PartitionNumber - 1 + i));

            PartitionNumber = PartitionNumber + diff;
        }
    
        internal MockPartition GetPartition(int partition)
        {
            if (PartitionNumber - 1 >= partition)
                return partitions[partition];
            else
            {
                this.CreateNewPartitions(new Partition(partition));
                return partitions[partition];
            }
        }
    }
}
