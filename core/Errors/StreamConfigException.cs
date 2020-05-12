using System;

namespace Streamiz.Kafka.Net.Errors
{
    /// <summary>
    /// Signals that the configuration in your stream is incorrect or maybe a property is missing
    /// </summary>
    public class StreamConfigException : Exception
    {
        /// <summary>
        /// Constructor with exception message
        /// </summary>
        /// <param name="message">Message</param>
        public StreamConfigException(string message)
            : base(message)
        {
        }
    }
}
