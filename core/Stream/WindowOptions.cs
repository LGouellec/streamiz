using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream
{
    public abstract class WindowOptions<W>
        where W : Window
    {

        public abstract IDictionary<long, W> WindowsFor(long timestamp);

        /// <summary>
        /// Return the size of the specified windows in milliseconds.
        /// </summary>
        public abstract long Size { get; }

        /// <summary>
        /// Return the window grace period (the time to admit out-of-order events after the end of the window.)
        /// </summary>
        public abstract long GracePeriodMs { get; }

    }
}
