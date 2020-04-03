using Confluent.Kafka;

namespace kafka_stream_core.Processors
{
    public interface ITimestampExtractor
    {
        long Extract(ConsumeResult<object, object> record, long partitionTime);
    }
}
