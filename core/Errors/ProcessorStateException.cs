using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Errors
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
