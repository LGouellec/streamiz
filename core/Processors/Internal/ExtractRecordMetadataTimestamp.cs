using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal abstract class ExtractRecordMetadataTimestamp : ITimestampExtractor
    {
        public long Extract(ConsumeResult<object, object> record, long partitionTime)
        {
            if (record.Message.Timestamp.UnixTimestampMs < 0)
            {
                return onInvalidTimestamp(record, record.Message.Timestamp.UnixTimestampMs, partitionTime);
            }

            return record.Message.Timestamp.UnixTimestampMs;
        }

        public abstract long onInvalidTimestamp(ConsumeResult<object, object> record, long recordTimestamp, long partitionTime);
    }
}
