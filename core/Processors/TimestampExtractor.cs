using Confluent.Kafka;

namespace kafka_stream_core.Processors
{
    public interface TimestampExtractor
    {
        long extract(ConsumeResult<object, object> record, long partitionTime);
    }
}
