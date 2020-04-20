using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Errors
{
    public class TopologyException : Exception
    {
        public TopologyException(string message) 
            : base(message)
        {
        }
    }
}
