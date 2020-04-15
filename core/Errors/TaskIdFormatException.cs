using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Errors
{
    public class TaskIdFormatException : Exception
    {
        public TaskIdFormatException(string message)
            : base($"Task id cannot be parsed correctly{(message == null ? "" : $" from {message}")}")
        {
        }
    }
}
