using System;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// Hopping time windows are windows based on time intervals. 
    /// They model fixed-sized, (possibly) overlapping windows. 
    /// A hopping window is defined by two properties: the window’s size and its advance interval (aka “hop”). 
    /// The advance interval specifies by how much a window moves forward relative to the previous one. 
    /// For example, you can configure a hopping window with a size 5 minutes and an advance interval of 1 minute. 
    /// Since hopping windows can overlap – and in general they do – a data record may belong to more than one such windows.
    /// </summary>
    public class HoppingWindowOptions : TimeWindowOptions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sizeMs"></param>
        /// <param name="advanceMs"></param>
        /// <param name="graceMs"></param>
        /// <param name="maintainDurationMs"></param>
        protected HoppingWindowOptions(long sizeMs, long advanceMs, long graceMs, long maintainDurationMs)
            : base(sizeMs, advanceMs, graceMs, maintainDurationMs)
        {
        }

        /// <summary>
        /// Static method to create <see cref="HoppingWindowOptions"/> with size windows and advance interval (aka “hop”)
        /// </summary>
        /// <param name="sizeMs">Size windows</param>
        /// <param name="advanceMs">Advance interval</param>
        /// <returns>Return a <see cref="HoppingWindowOptions"/> instance</returns>
        public static HoppingWindowOptions Of(long sizeMs, long advanceMs)
            => new HoppingWindowOptions(sizeMs, advanceMs, -1, DEFAULT_RETENTION_MS);

        /// <summary>
        /// Static method to create <see cref="HoppingWindowOptions"/> with size windows and advance interval (aka “hop”)
        /// </summary>
        /// <param name="sizeMs">Size windows</param>
        /// <param name="advanceMs">Advance interval</param>
        /// <param name="graceMs">the time to admit out-of-order events after the end of the window.</param>
        /// <returns>Return a <see cref="HoppingWindowOptions"/> instance</returns>
        public static HoppingWindowOptions Of(long sizeMs, long advanceMs,long graceMs)
            => new HoppingWindowOptions(sizeMs, advanceMs, graceMs, DEFAULT_RETENTION_MS);

        /// <summary>
        /// Static method to create <see cref="HoppingWindowOptions"/> with size windows and advance interval (aka “hop”)
        /// </summary>
        /// <param name="size">TimeSpan size windows</param>
        /// <param name="advance">Advance interval</param>
        /// <returns>Return a <see cref="HoppingWindowOptions"/> instance</returns>
        public static HoppingWindowOptions Of(TimeSpan size, TimeSpan advance)
            => Of((long)size.TotalMilliseconds, (long)advance.TotalMilliseconds);
        
        /// <summary>
        /// Static method to create <see cref="HoppingWindowOptions"/> with size windows and advance interval (aka “hop”)
        /// </summary>
        /// <param name="size">TimeSpan size windows</param>
        /// <param name="advance">Advance interval</param>
        /// <param name="grace">the time to admit out-of-order events after the end of the window.</param>
        /// <returns>Return a <see cref="HoppingWindowOptions"/> instance</returns>
        public static HoppingWindowOptions Of(TimeSpan size, TimeSpan advance,TimeSpan grace)
            => Of((long)size.TotalMilliseconds, (long)advance.TotalMilliseconds,(long)grace.TotalMilliseconds);
    }
}
