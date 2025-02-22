using System;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// A <see cref="TimeWindow"/> covers a half-open time interval with its start timestamp as an inclusive boundary and its end
    /// timestamp as exclusive boundary.
    /// It is a fixed size window, i.e., all instances (of a single <see cref="TimeWindowOptions"/> window specification) will have the same size.
    /// </summary>
    public class TimeWindow : Window
    {
        /// <summary>
        /// Create a new window for the given start time (inclusive) and end time (exclusive).
        /// </summary>
        /// <param name="startMs">the start timestamp of the window (inclusive)</param>
        /// <param name="endMs">the end timestamp of the window (exclusive)</param>
        /// <exception cref="ArgumentException">if <paramref name="startMs"/> is negative or if <paramref name="endMs"/> is smaller than or equal to <paramref name="startMs"/></exception>
        public TimeWindow(long startMs, long endMs)
            : base(startMs, endMs)
        {
        }

        /// <summary>
        /// Check if the given window overlaps with this window.
        /// </summary>
        /// <param name="other">another window</param>
        /// <returns>true if other overlaps with this window, false otherwise</returns>
        public override bool Overlap(Window other)
            => other is TimeWindow window && StartMs < window.EndMs && window.StartMs < EndMs;
    }
}
