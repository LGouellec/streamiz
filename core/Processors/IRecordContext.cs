using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors
{
    /// <summary>
    /// The context associated with the current record being processed by an <see cref="IProcessor"/>.
    /// </summary>
    public interface IRecordContext
    {
        /// <summary>
        /// Return offset of the current record
        /// </summary>
        long Offset { get; }
        /// <summary>
        /// Return timestamp of the current record
        /// </summary>
        long Timestamp { get; }
        /// <summary>
        /// Return topic name of the current record
        /// </summary>
        string Topic { get; }
        /// <summary>
        /// Return partition of the current record
        /// </summary>
        int Partition { get; }
        /// <summary>
        /// Return a collection of header of the current record
        /// </summary>
        Headers Headers { get; }

        /// <summary>
        /// Change current timestamp
        /// </summary>
        /// <param name="ts">new timestamp</param>
        void ChangeTimestamp(long ts);

        /// <summary>
        /// Change current list of headers
        /// </summary>
        /// <param name="headers">new headers</param>
        void SetHeaders(Headers headers);
    }
}
