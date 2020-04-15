using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Errors
{
    public class IllegalStateException : Exception
    {
        public IllegalStateException(string message) : base(message)
        {
        }

        public IllegalStateException(string message, Exception innerException) 
            : base(message, innerException)
        {
        }
    }
}
