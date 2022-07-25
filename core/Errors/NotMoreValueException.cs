using System;
using System.Collections;

namespace Streamiz.Kafka.Net.Errors
{
    /// <summary>
    /// Exception throws on a empty rocksdb enumerator when <see cref="IEnumerator.Current"/> is called.
    /// </summary>
    public class NotMoreValueException : Exception
    {
        /// <summary>
        /// Constructor with error message
        /// </summary>
        /// <param name="message">Exception message</param>
        public NotMoreValueException(string message) 
            : base(message)
        {
        }
    }
}
