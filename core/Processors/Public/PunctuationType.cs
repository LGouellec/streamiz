namespace Streamiz.Kafka.Net.Processors.Public
{
    /// <summary>
    /// Controls what notion of time is used for punctuation scheduled via <see cref="ProcessorContext{K,V}.Schedule"/>
    /// <para>
    ///   - STREAM_TIME - uses "stream time", which is advanced by the processing of messages
    ///   in accordance with the timestamp as extracted by the <see cref="ITimestampExtractor"/> in use.
    ///   NOTE: Only advanced if messages arrive
    ///   - PROCESSING_TIME - uses system time (the wall-clock time),
    ///   which is advanced at the polling interval (<see cref="IStreamConfig.PollMs"/>)
    ///   independent of whether new messages arrive.
    ///   NOTE: This is best effort only as its granularity is limited
    ///   by how long an iteration of the processing loop takes to complete
    /// </para>
    /// </summary>
    public enum PunctuationType
    {
        /// <summary>
        /// Use system time
        /// </summary>
        PROCESSING_TIME,
        /// <summary>
        /// Use stream time
        /// </summary>
        STREAM_TIME
    }
}