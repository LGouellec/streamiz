using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Errors
{
    public class TopologyException : Exception
    {
        public TopologyException(string message) 
            : base(message)
        {
        }
    }
}
