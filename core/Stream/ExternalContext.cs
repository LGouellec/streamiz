using System;
using System.Collections.ObjectModel;

namespace Streamiz.Kafka.Net.Stream
{
    public class ExternalContext
    {
        public int RetryNumber { get; internal set; }
        public long FirstCallEpoch { get; internal set; }
        public long CurrentCallEpoch { get; internal set; }
        public ReadOnlyCollection<Exception> LastExceptions { get; internal set; }
    }
}