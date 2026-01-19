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
        public MockWallClockTimeProvider()
        {
            // Initialize to current time
            WallClockTime = DateTime.Now.GetMilliseconds();
        }

        public long WallClockTime {get; private set;}

        public void Advance(TimeSpan advance)
        {
            WallClockTime += (long)advance.TotalMilliseconds;
        }
    }
}