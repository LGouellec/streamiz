using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Errors
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
