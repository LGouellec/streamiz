using System;
using System.Runtime.InteropServices;
using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.Processors.Public
{
    public class TaskScheduled
    {
        private long lastTime;
        private readonly TimeSpan interval;
        private readonly Action<long> punctuator;

        public TaskScheduled(long startTime, TimeSpan interval, Action<long> punctuator)
        {
            lastTime = startTime;
            this.interval = interval;
            this.punctuator = punctuator;
        }

        public void Cancel()
        {
            if (!IsCancelled)
                IsCancelled = true;
            else
                throw new StreamsException("This scheduled task is already cancelled !");
        }

        public bool IsCancelled { get; set; }
        public bool IsCompleted { get; set; }

        internal bool CanExecute(long now)
            => now - lastTime >= interval.Milliseconds;

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