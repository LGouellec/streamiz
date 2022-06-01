using System;

namespace Streamiz.Kafka.Net.Errors
{
    public class NoneRetryableException : Exception
    {
        public int NumberRetry { get; }
        public long ElapsedTime { get; }

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