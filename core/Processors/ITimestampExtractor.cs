using Confluent.Kafka;

namespace Kafka.Streams.Net.Processors
{
    public interface ITimestampExtractor
    {
        long Extract(ConsumeResult<object, object> record, long partitionTime);
    }
}
