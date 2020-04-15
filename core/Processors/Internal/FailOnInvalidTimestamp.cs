using Confluent.Kafka;
using Kafka.Streams.Net.Crosscutting;
using Kafka.Streams.Net.Errors;
using log4net;

namespace Kafka.Streams.Net.Processors.Internal
{
    internal class FailOnInvalidTimestamp : ExtractRecordMetadataTimestamp
    {
        private readonly ILog log = Logger.GetLogger(typeof(FailOnInvalidTimestamp));

        public override long onInvalidTimestamp(ConsumeResult<object, object> record, long recordTimestamp, long partitionTime)
        {
            var message = $"Input record {record} has invalid (negative) timestamp. Possibly because a pre-0.10 producer client was used to write this record to Kafka without embedding a timestamp, or because the input topic was created before upgrading the Kafka cluster to 0.10+. Use a different TimestampExtractor to process this data.";

            log.Error(message);
            throw new StreamsException(message);
        }
    }
}
