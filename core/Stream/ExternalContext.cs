using System;
using System.Collections.ObjectModel;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// External context during an asynchronous processing operation
    /// </summary>
    public class ExternalContext
    {
        /// <summary>
        /// Current retry number for current record
        /// </summary>
        public int RetryNumber { get; internal set; }
        /// <summary>
        /// First time of call in milliseconds (epoch)
        /// </summary>
        public long FirstCallEpoch { get; internal set; }
        /// <summary>
        /// Current time of call in milliseconds (epoch)
        /// </summary>
        public long CurrentCallEpoch { get; internal set; }
        /// <summary>
        /// Last exceptions thrown during the last processing iteration
        /// </summary>
        public ReadOnlyCollection<Exception> LastExceptions { get; internal set; }
    }
}