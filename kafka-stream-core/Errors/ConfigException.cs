using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Errors
{
    public class ConfigException : Exception
    {
        public ConfigException(string message) 
            : base(message)
        {
        }
    }
}
