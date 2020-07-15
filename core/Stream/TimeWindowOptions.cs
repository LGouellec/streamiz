using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// The fixed-size time-based window specifications used for aggregations.
    /// The semantics of time-based aggregation windows are: Every T1 (advance) milliseconds, compute the aggregate total for
    /// T2 (size) milliseconds.
    /// </summary>
    public abstract class TimeWindowOptions : WindowOptions<TimeWindow>
    {
        /// <summary>
        /// The size of the window's advance interval in milliseconds
        /// </summary>
        protected readonly long advanceMs;

        /// <summary>
        /// The grace of window's size in milliseconds
        /// </summary>
        protected readonly long graceMs;

        /// <summary>
        /// The maintain duration of window's in milliseconds
        /// </summary>
        protected readonly long maintainDurationMs;

        /// <summary>
        /// Protectec constructor
        /// </summary>
        /// <param name="sizeMs">Size ms</param>
        /// <param name="advanceMs">Advance ms</param>
        /// <param name="graceMs">Grace ms</param>
        /// <param name="maintainDurationMs">Maintain duration ms</param>
        protected TimeWindowOptions(long sizeMs, long advanceMs, long graceMs, long maintainDurationMs)
        {
            Size = sizeMs;
            this.advanceMs = advanceMs;
            this.graceMs = graceMs;
            this.maintainDurationMs = maintainDurationMs;
        }

        /// <summary>
        /// Size of the window
        /// </summary>
        public override long Size { get; }

        /// <summary>
        /// Return the window grace period (the time to admit out-of-order events after the end of the window.)
        /// Delay is defined as (stream_time - record_timestamp).
        /// </summary>
        public override long GracePeriodMs => graceMs != -1 ? graceMs : maintainDurationMs - Size;

        /// <summary>
        /// Create all windows that contain the provided timestamp, indexed by non-negative window start timestamps.
        /// </summary>
        /// <param name="timestamp">the timestamp window should get created for</param>
        /// <returns>a map of &lt;windowStartTimestamp, Window&gt; entries</returns>
        public override IDictionary<long, TimeWindow> WindowsFor(long timestamp)
        {
            long windowStart = (Math.Max(0, timestamp - Size + advanceMs) / advanceMs) * advanceMs;
            IDictionary<long, TimeWindow> windows = new Dictionary<long, TimeWindow>();
            while (windowStart <= timestamp)
            {
                TimeWindow window = new TimeWindow(windowStart, windowStart + Size);
                windows.Add(windowStart, window);
                windowStart += advanceMs;
            }
            return windows;
        }
    }
}
