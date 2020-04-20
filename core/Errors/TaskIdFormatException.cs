using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Errors
{
    public class TaskIdFormatException : Exception
    {
        public TaskIdFormatException(string message)
            : base($"Task id cannot be parsed correctly{(message == null ? "" : $" from {message}")}")
        {
        }
    }
}
