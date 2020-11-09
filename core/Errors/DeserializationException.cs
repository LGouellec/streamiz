using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Errors
{
    public class DeserializationException : Exception
    {
        public DeserializationException(string message, Exception innerException) :
            base(message, innerException)
        {
        }
    }
}
