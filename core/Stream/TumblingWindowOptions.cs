using System;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// Tumbling time windows are a special case of hopping time windows and, like the latter, are windows based on time intervals. 
    /// They model fixed-size, non-overlapping, gap-less windows. 
    /// A tumbling window is defined by a single property: the window’s size. 
    /// A tumbling window is a hopping window whose window size is equal to its advance interval.
    /// Since tumbling windows never overlap, a data record will belong to one and only one window.
    /// </summary>
    public class TumblingWindowOptions : TimeWindowOptions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sizeMs"></param>
        /// <param name="advanceMs"></param>
        /// <param name="graceMs"></param>
        /// <param name="maintainDurationMs"></param>
        protected TumblingWindowOptions(long sizeMs, long advanceMs, long graceMs, long maintainDurationMs)
            : base(sizeMs, advanceMs, graceMs, maintainDurationMs)
        {
        }

        /// <summary>
        /// Static method to create <see cref="TumblingWindowOptions"/> with size windows.
        /// </summary>
        /// <param name="sizeMs">Size windows</param>
        /// <returns>Return a <see cref="TumblingWindowOptions"/> instance</returns>
        public static TumblingWindowOptions Of(long sizeMs)
            => new TumblingWindowOptions(sizeMs, sizeMs, -1, DEFAULT_RETENTION_MS);

        /// <summary>
        /// Static method to create <see cref="TumblingWindowOptions"/> with size windows.
        /// </summary>
        /// <param name="sizeMs">Size windows</param>
        /// <param name="graceMs">the time to admit out-of-order events after the end of the window.</param>
        /// <returns>Return a <see cref="TumblingWindowOptions"/> instance</returns>
        public static TumblingWindowOptions Of(long sizeMs, long graceMs)
            => new TumblingWindowOptions(sizeMs, sizeMs, graceMs, DEFAULT_RETENTION_MS);

        /// <summary>
        /// Static method to create <see cref="TumblingWindowOptions"/> with size windows.
        /// </summary>
        /// <param name="size">TimeSpan size windows</param>
        /// <returns>Return a <see cref="TumblingWindowOptions"/> instance</returns>
        public static TumblingWindowOptions Of(TimeSpan size)
            => Of((long)size.TotalMilliseconds);

        /// <summary>
        /// Static method to create <see cref="TumblingWindowOptions"/> with size windows.
        /// </summary>
        /// <param name="size">TimeSpan size windows</param>
        /// <param name="grace">the time to admit out-of-order events after the end of the window.</param>
        /// <returns>Return a <see cref="TumblingWindowOptions"/> instance</returns>
        public static TumblingWindowOptions Of(TimeSpan size, TimeSpan grace)
            => Of((long)size.TotalMilliseconds, (long)grace.TotalMilliseconds);
    }
}