using System.Diagnostics;

namespace Streamiz.Kafka.Net.Crosscutting
{
    /// <summary>
    /// Monotonic clock used to compute timeouts, intervals, deadlines and durations.
    /// The value returned by <see cref="NowMs"/> is not an epoch timestamp: it only makes sense when compared
    /// with another value returned by the same clock.
    /// Contrary to <see cref="System.DateTime.Now"/> (or <see cref="System.DateTime.UtcNow"/>), it is never
    /// affected by a system clock change (daylight saving time, NTP synchronization, manual adjustment).
    /// </summary>
    internal interface IMonotonicClock
    {
        /// <summary>
        /// Current value of the clock in milliseconds.
        /// </summary>
        long NowMs { get; }
    }

    /// <summary>
    /// Default <see cref="IMonotonicClock"/> implementation, based on <see cref="Stopwatch"/>.
    /// </summary>
    internal sealed class MonotonicClock : IMonotonicClock
    {
        private static readonly Stopwatch stopwatch = Stopwatch.StartNew();

        /// <summary>
        /// Shared instance
        /// </summary>
        internal static readonly MonotonicClock Instance = new();

        private MonotonicClock()
        {
        }

        /// <summary>
        /// Current value of the shared monotonic clock in milliseconds.
        /// </summary>
        internal static long CurrentMs => stopwatch.ElapsedMilliseconds;

        /// <summary>
        /// Number of milliseconds elapsed since <paramref name="startMs"/> (a value previously returned by <see cref="CurrentMs"/>).
        /// </summary>
        internal static long ElapsedSince(long startMs) => CurrentMs - startMs;

        public long NowMs => CurrentMs;
    }
}
