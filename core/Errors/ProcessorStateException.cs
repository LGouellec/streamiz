using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Errors
{
    public class ProcessorStateException : Exception
    {
        public ProcessorStateException(string message) : base(message)
        {
        }

        public ProcessorStateException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
