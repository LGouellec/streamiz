using System;

namespace Streamiz.Kafka.Net.Errors
{
    internal class NoneRetryableException : Exception
    {
        private int NumberRetry { get; }
        private long ElapsedTime { get; }

        public NoneRetryableException(
            string message,
            int numberRetry,
            long ellapsedTime,
            Exception innerException) 
            : base(message, innerException)
        {
            NumberRetry = numberRetry;
            ElapsedTime = ellapsedTime;
        }
    }
}