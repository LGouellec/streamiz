using System;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Mock
{
    /// <summary>
    /// A mock wall-clock time provider for testing that starts at the current time
    /// and allows manual advancement.
    /// </summary>
    internal class MockWallClockTimeProvider : IWallClockTimeProvider
    {
        private long _currentTimeMs;

        public MockWallClockTimeProvider()
        {
            // Initialize to current time
            _currentTimeMs = DateTime.Now.GetMilliseconds();
        }

        public long GetWallClockTime() => _currentTimeMs;

        public void Advance(TimeSpan advance)
        {
            _currentTimeMs += (long)advance.TotalMilliseconds;
        }
    }
}