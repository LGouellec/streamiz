using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Errors
{
    internal class TaskMigratedException : Exception
    {
        public TaskMigratedException(string message) 
            : base(message)
        {
        }
    }
}
