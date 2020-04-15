using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Errors
{
    public class StreamsException : Exception
    {

        public StreamsException(string message) 
            : base(message)
        {
        }

        public StreamsException(Exception innerException) 
            : this("", innerException)
        {
        }

        public StreamsException(string message, Exception innerException) 
            : base(message, innerException)
        {
        }
    }
}
