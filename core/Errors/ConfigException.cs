using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Errors
{
    public class ConfigException : Exception
    {
        public ConfigException(string message) 
            : base(message)
        {
        }
    }
}
