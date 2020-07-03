using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// The window specification for fixed size windows that is used to define window boundaries and grace period.
    /// <p>
    /// Grace period defines how long to wait on out-of-order events. That is, windows will continue to accept new records until stream_time >= window_end + grace_period.
    /// Records that arrive after the grace period passed are considered late and will not be processed but are dropped.
    /// </p>
    /// Warning: It may be unsafe to use objects of this class in set- or map-like collections, since the equals and hashCode methods depend on mutable fields.
    /// </summary>
    /// <typeparam name="W">type of the window instance</typeparam>
    public abstract class WindowOptions<W>
        where W : Window
    {
        /// <summary>
        /// Create all windows that contain the provided timestamp, indexed by non-negative window start timestamps.
        /// </summary>
        /// <param name="timestamp">the timestamp window should get created for</param>
        /// <returns>a map of &lt;windowStartTimestamp, Window&gt; entries</returns>
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
