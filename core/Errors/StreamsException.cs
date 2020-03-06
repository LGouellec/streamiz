using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Errors
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
