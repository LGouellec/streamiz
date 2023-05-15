using System;
using System.Runtime.InteropServices;
using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.Processors.Public
{
    /// <summary>
    /// Task scheduled cancellable returned in <see cref="ProcessorContext{K,V}.Schedule"/>
    /// </summary>
    public class TaskScheduled
    {
        private long lastTime;
        private readonly TimeSpan interval;
        private readonly Action<long> punctuator;

        internal TaskScheduled(long startTime, TimeSpan interval, Action<long> punctuator, IProcessor processor)
        {
            lastTime = startTime;
            this.interval = interval;
            this.punctuator = punctuator;
            Processor = processor;
        }

        /// <summary>
        /// Cancel the scheduled operation to avoid future calls.
        /// </summary>
        /// <exception cref="StreamsException">If the task is already cancelled</exception>
        public void Cancel()
        {
            if (!IsCancelled)
                IsCancelled = true;
            else
                throw new StreamsException("This scheduled task is already cancelled !");
        }

        /// <summary>
        /// Return if the task is cancelled
        /// </summary>
        public bool IsCancelled { get; set; }
        
        /// <summary>
        /// Return if the task is completed
        /// </summary>
        public bool IsCompleted { get; set; }

        internal IProcessor Processor { get; }

        internal bool CanExecute(long now)
            => now - lastTime >= interval.TotalMilliseconds;

        internal bool Execute(long now)
        {
            if (!IsCompleted && CanExecute(now))
            {
                punctuator(now);
                lastTime = now;
                return true;
            }

            return false;
        }

        internal void Close()
        {
            IsCompleted = true;
        }
        
    }
}