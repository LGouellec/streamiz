using System;

namespace Streamiz.Kafka.Net.Errors
{
    /// <summary>
    /// Signals that a method has been invoked at an illegal or inappropriate time.
    /// </summary>
    public class IllegalStateException : Exception
    {
        /// <summary>
        /// Constructor of IllegalStateException
        /// </summary>
        /// <param name="message">Exception message</param>
        public IllegalStateException(string message) : base(message)
        {
        }

        /// <summary>
        /// Constructor of IllegalStateException
        /// </summary>
        /// <param name="message">Exception message</param>
        /// <param name="innerException">Inner exception</param>
        public IllegalStateException(string message, Exception innerException) 
            : base(message, innerException)
        {
        }
    }
}
