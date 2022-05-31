using System;

namespace Streamiz.Kafka.Net.Errors
{
    public class NoneRetriableException : Exception
    {
        public int NumberRetry { get; }
        public long ElapsedTime { get; }

        public NoneRetriableException(
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