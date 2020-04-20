using Confluent.Kafka;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Processors
{
    /// <summary>
    /// An interface that allows the Kafka Streams framework to extract a timestamp from an instance of <see cref="ConsumeResult{TKey, TValue}"/>.
    /// The extracted timestamp is defined as milliseconds.
    /// </summary>
    public interface ITimestampExtractor
    {
        /// <summary>
        /// Extracts a timestamp from a record. The timestamp must be positive to be considered a valid timestamp.
        /// Returning a negative timestamp will cause the record not to be processed but rather silently skipped.
        /// In case the record contains a negative timestamp and this is considered a fatal error for the application,
        /// throwing an exception instead of returning the timestamp is a valid option too.
        /// For this case, Streams will stop processing and shut down to allow you investigate in the root cause of the
        /// negative timestamp.
        /// The timestamp extractor implementation must be stateless.
        /// The extracted timestamp MUST represent the milliseconds since midnight, January 1, 1970 UTC.
        /// It is important to note that this timestamp may become the message timestamp for any messages sent to changelogs
        /// updated by <see cref="IKTable{K, V}"/>s and joins.
        /// The message timestamp is used for log retention and log rolling, so using nonsensical values may result in
        /// excessive log rolling and therefore broker performance degradation.
        /// </summary>
        /// <param name="record">a data record</param>
        /// <param name="partitionTime">the highest extracted valid timestamp of the current record's partition˙</param>
        /// <returns>the timestamp of the record</returns>
        long Extract(ConsumeResult<object, object> record, long partitionTime);
    }
}
