using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Errors
{
    public class TopologyException : Exception
    {
        public TopologyException(string message) 
            : base(message)
        {
        }
    }
}
