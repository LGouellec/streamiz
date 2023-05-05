using System;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Processors.Public
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <typeparam name="V"></typeparam>
    public class ProcessorContext<K, V> : ProcessorContext
    {
        private readonly ProcessorContext context;

        internal ProcessorContext(ProcessorContext context)
        {
            this.context = context;
        }

        #region Override

        internal override IStreamConfig Configuration => context.Configuration;
        
        /// <summary>
        /// Current task id of processing
        /// </summary>
        public override TaskId Id => context.Id;
        
        /// <summary>
        /// Return the <see cref="StreamMetricsRegistry"/> instance.
        /// </summary>
        public override StreamMetricsRegistry Metrics => context.Metrics;
        internal override IStateManager States => context.States;
        internal override bool FollowMetadata => context.FollowMetadata;
        internal override IProcessor CurrentProcessor => context.CurrentProcessor;
        internal override IRecordContext RecordContext => context.RecordContext;

        #endregion

        private void CheckConfiguration()
        {
            if (Configuration.ParallelProcessing)
                throw new StreamConfigException("The forwarder is disabled when the parallel processing is enabled.");
        }
        
        /// <summary>
        /// Forward a new key/value pair to all downstream processors 
        /// </summary>
        /// <param name="key">new key</param>
        /// <param name="value">new value</param>
        /// <exception cref="StreamConfigException">the forwarding is disabled if the parallel processing is enabled</exception>
        public void Forward(K key, V value)
        {
            CheckConfiguration();
            var oldProcessor = CurrentProcessor;
            CurrentProcessor.Forward(key, value);
            context.CurrentProcessor = oldProcessor;
        }

        /// <summary>
        /// Forward a new key/value pair to the downstream processor <see cref="named"/>.
        /// </summary>
        /// <param name="key">new key</param>
        /// <param name="value">new value</param>
        /// <param name="named">Name of the downstream processor</param>
        /// <exception cref="StreamConfigException">the forwarding is disabled if the parallel processing is enabled</exception>
        public void Forward(K key, V value, string named)
        {
            CheckConfiguration();
            var oldProcessor = CurrentProcessor;
            CurrentProcessor.Forward(key, value, named);
            context.CurrentProcessor = oldProcessor;
        }
        
        /// <summary>
        /// Requests a commit
        /// </summary>
        public virtual void Commit() 
            => context.Task.RequestCommit();

        
        /// <summary>
        /// Schedule a periodic operation for processors. A processor may call this method during
        /// <see cref="IProcessor{K,V}.Init"/> or <see cref="IProcessor{K,V}.Process"/> to
        /// schedule a periodic callback.
        /// The type parameter controls what notion of time is used for punctuation:
        /// <para>
        ///  - <see cref="PunctuationType.STREAM_TIME"/> uses "stream time", which is advanced by the processing of messages
        ///   in accordance with the timestamp as extracted by the <see cref="ITimestampExtractor"/> in use.
        ///  The first punctuation will be triggered by the first record that is processed (NOTE: Only advanced if messages arrive).
        ///  - <see cref="PunctuationType.PROCESSING_TIME"/> uses system time (the wall-clock time), which is advanced independent of whether new messages arrive.
        ///   The first punctuation will be triggered after interval has elapsed (NOTE: This is best effort only as its granularity is limited by how long an iteration of the
        ///   processing loop takes to complete).
        /// </para>
        /// </summary>
        /// <param name="interval">the time interval between punctuations (supported minimum is 10 millisecond)</param>
        /// <param name="punctuationType">type of <see cref="PunctuationType"/></param>
        /// <param name="punctuator">a function consuming timestamps representing the current stream or system time</param>
        /// <returns>the task scheduled allowing cancellation of the punctuation schedule established by this method</returns>
        /// <exception cref="ArgumentException">if the interval is not representable in milliseconds or less than 10 milliseconds</exception>
        public TaskScheduled Schedule(TimeSpan interval, PunctuationType punctuationType, Action<long> punctuator)
        {
            if (interval.TotalMilliseconds < 10)
                throw new ArgumentException("The minimum supported scheduling interval is 10 milliseconds.");
            
            return context.Task.RegisterScheduleTask(interval, punctuationType, punctuator);   
        }
    }
}