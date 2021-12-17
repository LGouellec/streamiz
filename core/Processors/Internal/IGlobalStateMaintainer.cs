using System.Collections.Generic;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal interface IGlobalStateMaintainer
    {
        public void Update(ConsumeResult<byte[], byte[]> record);

        public void FlushState();

        public void Close();

        public IDictionary<TopicPartition, long> Initialize();
    }
}
