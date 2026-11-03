using System;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors.Public;

namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// Listener invoked by the stream thread around the processing of each consumed record and around each punctuation.
    /// It can be used to start a tracing span (see <see cref="System.Diagnostics.Activity"/>), push a logging scope or
    /// measure processing without changing the topology.
    /// <para>
    /// The start and end callbacks are invoked synchronously on the processing thread, so any ambient state set in
    /// the start callback (e.g. <see cref="System.Diagnostics.Activity.Current"/>) is visible to every processor
    /// of the sub-topology, including the sink nodes, until the end callback.
    /// </para>
    /// <para>
    /// The listener only observes the processing and must not modify the record or the context.
    /// An exception thrown during the processing is passed to the end callback and then rethrown, so the configured
    /// exception handlers are applied as usual. An exception thrown by the listener itself is logged and ignored.
    /// Implementations must be thread-safe: one instance is shared by all stream threads.
    /// </para>
    /// </summary>
    public interface IProcessingListener
    {
        /// <summary>
        /// Called before a consumed record is deserialized and forwarded through the sub-topology.
        /// Record metadata (topic, partition, offset, timestamp, headers) is already set on the <paramref name="context"/>.
        /// This is called for records of source topics, internal repartition topics, table and global table source
        /// topics and request topics of async processors. It is not called while restoring state stores.
        /// </summary>
        /// <param name="context">Processor context of the task processing the record</param>
        /// <param name="record">Consumed record (raw bytes)</param>
        /// <returns>State passed to <see cref="OnProcessEnd"/>, may be null</returns>
        object OnProcessStart(ProcessorContext context, ConsumeResult<byte[], byte[]> record);

        /// <summary>
        /// Called when the processing of the record started with <see cref="OnProcessStart"/> is completed.
        /// </summary>
        /// <param name="state">State returned by <see cref="OnProcessStart"/>, null if it failed</param>
        /// <param name="exception">Exception thrown during the processing, null if succeeded</param>
        void OnProcessEnd(object state, Exception exception);

        /// <summary>
        /// Called before a punctuator scheduled with <see cref="ProcessorContext{K,V}.Schedule"/> is executed.
        /// </summary>
        /// <param name="context">Processor context of the task</param>
        /// <param name="processorName">Name of the processor which scheduled the punctuation</param>
        /// <param name="punctuationType">Type of punctuation</param>
        /// <param name="timestamp">Stream time or wall clock time passed to the punctuator</param>
        /// <returns>State passed to <see cref="OnPunctuateEnd"/>, may be null</returns>
        object OnPunctuateStart(ProcessorContext context, string processorName, PunctuationType punctuationType, long timestamp);

        /// <summary>
        /// Called when the punctuation started with <see cref="OnPunctuateStart"/> is completed.
        /// </summary>
        /// <param name="state">State returned by <see cref="OnPunctuateStart"/>, null if it failed</param>
        /// <param name="exception">Exception thrown by the punctuator, null if succeeded</param>
        void OnPunctuateEnd(object state, Exception exception);
    }
}
