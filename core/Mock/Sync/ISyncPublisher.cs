using System;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    internal interface ISyncPublisher
    {
        public void PublishRecord(string topic, byte[] key, byte[] value, DateTime timestamp);
        public void Flush();
    }
}