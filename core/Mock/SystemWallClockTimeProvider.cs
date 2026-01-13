using System;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Mock
{
    /// <summary>
    /// Default wall-clock time provider that returns the actual system time.
    /// Used in production when no mock time provider is injected.
    /// </summary>
    internal class SystemWallClockTimeProvider : IWallClockTimeProvider
    {
        /// <summary>
        /// Singleton instance for shared use.
        /// </summary>
        public static readonly SystemWallClockTimeProvider Instance = new();

        public long WallClockTime => DateTime.Now.GetMilliseconds();

        /// <summary>
        /// No-op for system time provider. Time advances naturally.
        /// </summary>
        public void Advance(TimeSpan advance)
        {
            // No-op: system time cannot be advanced
        }
    }
}
