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
        public override TaskId Id => context.Id;
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
            CurrentProcessor.Forward(key, value);
        }

        /// <summary>
        /// Forward a new key/value pair to the downstream processor <seealso cref="named"/>.
        /// </summary>
        /// <param name="key">new key</param>
        /// <param name="value">new value</param>
        /// <param name="named">Name of the downstream processor</param>
        /// <exception cref="StreamConfigException">the forwarding is disabled if the parallel processing is enabled</exception>
        public void Forward(K key, V value, string named)
        {
            CheckConfiguration();
            CurrentProcessor.Forward(key, value, named);
        }
        
        /// <summary>
        /// Requests a commit
        /// </summary>
        public virtual void Commit() 
            => context.Task.RequestCommit();

        public TaskScheduled Schedule(TimeSpan interval, PunctuationType punctuationType, Action<long> punctuator)
        {
            if (interval.TotalMilliseconds < 10)
                throw new ArgumentException("The minimum supported scheduling interval is 10 milliseconds.");
            
            return context.Task.RegisterScheduleTask(interval, punctuationType, punctuator);   
        }
    }
}