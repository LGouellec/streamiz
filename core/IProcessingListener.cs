using System;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors.Public;

namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// Listener invoked by the stream thread around the processing of each consumed record and around each punctuation.
    /// <para>
    /// It allows users or libraries to observe the processing without changing the topology, for instance to start a
    /// tracing span (<c>System.Diagnostics.Activity</c>), push a logging scope or measure the processing.
    /// Streamiz itself does not depend on any tracing library.
    /// </para>
    /// <para>
    /// Behavior :
    /// <list type="bullet">
    /// <item>The callbacks are invoked synchronously on the processing thread. Every ambient value set in a start callback
    /// (e.g. <c>Activity.Current</c>, <c>AsyncLocal</c>) is visible from all processors of the sub-topology,
    /// from the deserialization of the source up to the serialization in the sink. Note that a record is also deserialized
    /// once when it is buffered (timestamp extraction), before <see cref="OnProcessStart"/>.</item>
    /// <item>Record processing is observed for source topics, repartition topics, table and global table sources and
    /// async processor request/response topics. It is not invoked during state store restoration.</item>
    /// <item>The listener only observes : it must not modify the record or the context.</item>
    /// <item>An exception thrown during the processing is passed to the end callback and then rethrown unchanged, so
    /// <see cref="IStreamConfig.InnerExceptionHandler"/> and <see cref="IStreamConfig.DeserializationExceptionHandler"/>
    /// behave as without a listener.</item>
    /// <item>An exception thrown by the listener itself is logged as a warning and ignored : it never fails the record
    /// and never replaces the processing exception. If a start callback throws, the matching end callback is not invoked.</item>
    /// </list>
    /// </para>
    /// </summary>
    public interface IProcessingListener
    {
        /// <summary>
        /// Invoked before processing a consumed record (before its deserialization).
        /// </summary>
        /// <param name="context">Processor context of the task, initialized with the metadata of <paramref name="record"/></param>
        /// <param name="record">Raw consumed record</param>
        /// <returns>A state object passed to <see cref="OnProcessEnd"/> (may be null)</returns>
        object OnProcessStart(ProcessorContext context, ConsumeResult<byte[], byte[]> record);

        /// <summary>
        /// Invoked after the processing of a record, even if the processing failed.
        /// </summary>
        /// <param name="state">State returned by <see cref="OnProcessStart"/></param>
        /// <param name="exception">Exception thrown during the processing, null if the processing succeeded</param>
        void OnProcessEnd(object state, Exception exception);

        /// <summary>
        /// Invoked before executing a punctuator.
        /// </summary>
        /// <param name="context">Processor context of the task</param>
        /// <param name="processorName">Name of the processor which scheduled the punctuator</param>
        /// <param name="punctuationType">Punctuation type</param>
        /// <param name="timestamp">Stream time or wall clock time (depending on <paramref name="punctuationType"/>) passed to the punctuator</param>
        /// <returns>A state object passed to <see cref="OnPunctuateEnd"/> (may be null)</returns>
        object OnPunctuateStart(ProcessorContext context, string processorName, PunctuationType punctuationType, long timestamp);

        /// <summary>
        /// Invoked after the execution of a punctuator, even if it failed.
        /// </summary>
        /// <param name="state">State returned by <see cref="OnPunctuateStart"/></param>
        /// <param name="exception">Exception thrown by the punctuator, null if it succeeded</param>
        void OnPunctuateEnd(object state, Exception exception);
    }
}
