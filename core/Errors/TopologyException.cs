using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;
using System;

namespace Streamiz.Kafka.Net.Errors
{
    /// <summary>
    /// Indicates a pre run time error occurred while parsing the <see cref="Topology"/> logical topology
    /// to construct the <see cref="ProcessorTopology"/> physical processor topology.
    /// </summary>
    public class TopologyException : Exception
    {
        /// <summary>
        /// Constructor with exception message
        /// </summary>
        /// <param name="message">Exception message</param>
        public TopologyException(string message) 
            : base(message)
        {
        }
    }
}
