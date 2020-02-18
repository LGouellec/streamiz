using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Errors
{
    public class TaskIdFormatException : Exception
    {
        public TaskIdFormatException(string message)
            : base($"Task id cannot be parsed correctly{(message == null ? "" : $" from {message}")}")
        {
        }
    }
}
