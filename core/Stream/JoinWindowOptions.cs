using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// The window specifications used for joins.
    /// A <see cref="JoinWindowOptions"/> instance defines a maximum time difference for a <see cref="IKStream{K, V}.Join{V0, VR, V0S, VRS}(IKStream{K, V0}, Func{V, V0, VR}, JoinWindowOptions, StreamJoinProps)"/> join over two streams, on the same key.
    /// In SQL-style you would express this join as :
    /// <para>
    /// SELECT* FROM stream1, stream2
    ///     WHERE
    ///       stream1.key = stream2.key
    ///       AND
    ///       stream1.ts - before &lt;= stream2.ts AND stream2.ts &lt;= stream1.ts + after
    /// </para>
    /// There are three different window configuration supported:
    /// <ul>
    ///     <li>before = after = time-difference</li>
    ///     <li>before = 0 and after = time-difference</li>
    ///     <li>before = time-difference and after = 0</li>
    /// </ul>
    /// A join is symmetric in the sense, that a join specification on the first stream returns the same result record as
    /// a join specification on the second stream with flipped before and after values.
    /// <p>
    /// Both values (before and after) must not result in an "inverse" window, i.e., upper-interval bound cannot be smaller
    /// than lower-interval bound.
    /// </p>
    /// <see cref="JoinWindowOptions"/> are sliding windows, thus, they are aligned to the actual record timestamps.
    /// This implies, that each input record defines its own window with start and end time being relative to the record's
    /// timestamp.
    /// </summary>
    public class JoinWindowOptions : WindowOptions<Window>
    {
        internal readonly long beforeMs;
        internal readonly long afterMs;
        private readonly long graceMs;
        private readonly long maintainDurationMs;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="beforeMs"></param>
        /// <param name="afterMs"></param>
        /// <param name="graceMs"></param>
        /// <param name="maintainDurationMs"></param>
        protected JoinWindowOptions(long beforeMs, long afterMs, long graceMs, long maintainDurationMs)
        {
            this.beforeMs = beforeMs;
            this.afterMs = afterMs;
            this.graceMs = graceMs;
            this.maintainDurationMs = Math.Max(maintainDurationMs, Size);
        }

        /// <summary>
        /// Size of window
        /// </summary>
        public override long Size => beforeMs + afterMs;

        /// <summary>
        /// Period when rejecting out-of-order events 
        /// </summary>
        public override long GracePeriodMs => graceMs != -1 ? graceMs : maintainDurationMs - Size;

        /// <summary>
        /// Not supported by <see cref="JoinWindowOptions"/>.
        /// </summary>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        public override IDictionary<long, Window> WindowsFor(long timestamp)
        {
            throw new NotImplementedException("WindowsFor() is not supported by JoinWindows");
        }

        #region Static

        /// <summary>
        /// Specifies that records of the same key are joinable if their timestamps are within timeDifferenceMs,
        /// i.e., the timestamp of a record from the secondary stream is max timeDifferenceMs earlier or later than
        /// the timestamp of the record from the primary stream.
        /// </summary>
        /// <param name="timeDifferenceMs">join window interval in milliseconds</param>
        /// <returns>Return <see cref="JoinWindowOptions"/> instance</returns>
        public static JoinWindowOptions Of(long timeDifferenceMs)
        {
            // This is a static factory method, so we initialize grace and retention to the defaults.
            return new JoinWindowOptions(timeDifferenceMs, timeDifferenceMs, -1L, DEFAULT_RETENTION_MS);
        }

        /// <summary>
        /// Specifies that records of the same key are joinable if their timestamps are within timeDifferenceMs,
        /// i.e., the timestamp of a record from the secondary stream is max timeDifferenceMs earlier or later than 
        /// the timestamp of the record from the primary stream.
        /// </summary>
        /// <param name="timeDifferenceMs">join window interval in milliseconds</param>
        /// <param name="graceMs">the time to admit out-of-order events after the end of the window.</param>
        /// <returns>Return <see cref="JoinWindowOptions"/> instance</returns>
        public static JoinWindowOptions Of(long timeDifferenceMs, long graceMs)
        {
            if (graceMs < 0)
            {
                throw new ArgumentOutOfRangeException("Grace period must not be negative.");
            }

            // This is a static factory method, so we initialize grace and retention to the defaults.
            return new JoinWindowOptions(timeDifferenceMs, timeDifferenceMs, graceMs, DEFAULT_RETENTION_MS);
        }

        /// <summary>
        /// Specifies that records of the same key are joinable if their timestamps are within timeDifference,
        /// i.e., the timestamp of a record from the secondary stream is max timeDifference earlier or later than
        /// the timestamp of the record from the primary stream.
        /// </summary>
        /// <param name="timeDifference">join window interval</param>
        /// <returns></returns>
        public static JoinWindowOptions Of(TimeSpan timeDifference)
            => Of((long)timeDifference.TotalMilliseconds);
        
        /// <summary>
        /// Specifies that records of the same key are joinable if their timestamps are within timeDifference,
        /// i.e., the timestamp of a record from the secondary stream is max timeDifference earlier or later than
        /// the timestamp of the record from the primary stream.
        /// </summary>
        /// <param name="timeDifference">join window interval</param>
        /// <param name="grace">the time to admit out-of-order events after the end of the window.</param>
        /// <returns></returns>
        public static JoinWindowOptions Of(TimeSpan timeDifference,TimeSpan grace)
            => Of((long)timeDifference.TotalMilliseconds,(long)grace.TotalMilliseconds);

        #endregion
    }
}