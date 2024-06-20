using System;

namespace Streamiz.Kafka.Net.State.Cache.Internal
{
    internal interface IClockTime
    {
        DateTime GetCurrentTime();
    }

    internal class ClockSystemTime : IClockTime
    {
        public DateTime GetCurrentTime() => DateTime.Now;
    }


    internal class MockSystemTime : IClockTime
    {
        private DateTime initialTime;
        private DateTime currentTime;

        public MockSystemTime(DateTime initialTime)
        {
            this.initialTime = initialTime;
            currentTime = initialTime;
        }

        public void AdvanceTime(TimeSpan timeSpan) => currentTime = currentTime.Add(timeSpan);
        public void ReduceTime(TimeSpan timeSpan) => currentTime = currentTime.Subtract(timeSpan);
        public DateTime GetCurrentTime() => currentTime;
    }
}