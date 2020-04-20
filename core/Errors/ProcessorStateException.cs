using System;

namespace Streamiz.Kafka.Net.Errors
{
    /// <summary>
    /// Indicates a processor state operation (e.g. put, get) has failed.
    /// </summary>
    public class ProcessorStateException : Exception
    {
        /// <summary>
        /// Constructor with message
        /// </summary>
        /// <param name="message">Exception message</param>
        public ProcessorStateException(string message) : base(message)
        {
        }

        /// <summary>
        /// Constructor with message and innerexception
        /// </summary>
        /// <param name="message">Exception message</param>
        /// <param name="innerException">Inner exception</param>
        public ProcessorStateException(string message, Exception innerException) 
            : base(message, innerException)
        {
        }
    }
}
