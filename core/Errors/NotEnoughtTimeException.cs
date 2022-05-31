using System;

namespace Streamiz.Kafka.Net.Errors
{
    public class NotEnoughtTimeException : Exception
    {
        public long ElapsedTime { get; }

        public NotEnoughtTimeException(string message, long elapsedTime) 
            : base(message)
        {
            ElapsedTime = elapsedTime;
        }
    }
}