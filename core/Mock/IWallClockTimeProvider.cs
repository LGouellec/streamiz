using System;

namespace Streamiz.Kafka.Net.Mock
{
    /// <summary>
    /// Interface for providing wall-clock time. Used to allow time mocking in tests.
    /// </summary>
    internal interface IWallClockTimeProvider
    {
        /// <summary>
        /// Gets the current wall-clock time in milliseconds since epoch.
        /// </summary>
        long WallClockTime { get; }

        /// <summary>
        /// Advances the wall-clock time by the specified duration.
        /// Only applicable for mock implementations; production implementations may ignore this.
        /// </summary>
        /// <param name="advance">The amount of time to advance</param>
        void Advance(TimeSpan advance);
    }
}