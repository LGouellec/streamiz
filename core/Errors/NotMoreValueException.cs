using System;

namespace Streamiz.Kafka.Net.Errors
{
    public class NotMoreValueException : Exception
    {
        public NotMoreValueException(string message) 
            : base(message)
        {
        }
    }
}
