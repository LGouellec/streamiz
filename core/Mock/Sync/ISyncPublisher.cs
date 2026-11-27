using Confluent.Kafka;
using System;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    internal interface ISyncPublisher
    {
        public void PublishRecord(string topic, int partition, byte[] key, byte[] value, DateTime timestamp, Headers headers);
        public void Flush();
        public void Close();
    }
}