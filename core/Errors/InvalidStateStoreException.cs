using Streamiz.Kafka.Net.Processors;
using System;

namespace Streamiz.Kafka.Net.Errors
{
    /// <summary>
    /// Indicates that there was a problem when trying to access a  <see cref="IStateStore"/>, i.e, the Store is no longer 
    /// valid because it is closed or doesn't exist any more due to a rebalance.
    /// 
    /// These exceptions may be transient, i.e., during a rebalance it won't be possible to query the stores as they are
    /// being(re)-initialized. Once the rebalance has completed the stores will be available again. Hence, it is valid
    /// to backoff and retry when handling this exception.
    /// 
    /// </summary>
    public class InvalidStateStoreException : Exception
    {
        /// <summary>
        /// Constructor with exception message
        /// </summary>
        /// <param name="message">Exception message</param>
        public InvalidStateStoreException(string message) 
            : base(message)
        {
        }

        /// <summary>
        /// Constructor with inner exception
        /// </summary>
        /// <param name="innerException">Inner exception</param>
        public InvalidStateStoreException(Exception innerException) 
            : this("", innerException)
        {
        }

        /// <summary>
        /// Constructor with exception message and inner exception
        /// </summary>
        /// <param name="message">Exception message</param>
        /// <param name="innerException">Inner exception</param>
        public InvalidStateStoreException(string message, Exception innerException) 
            : base(message, innerException)
        {
        }
    }
}
