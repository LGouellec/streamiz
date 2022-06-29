using System;

namespace Streamiz.Kafka.Net.Errors
{
    internal class NotEnoughtTimeException : Exception
    {
        private long ElapsedTime { get; }

        public NotEnoughtTimeException(string message, long elapsedTime) 
            : base(message)
        {
            ElapsedTime = elapsedTime;
        }
    }
}