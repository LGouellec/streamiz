using System;

namespace Streamiz.Kafka.Net.Stream
{
    public class TimeWindow : Window
    {
        public TimeWindow(long startMs, long endMs)
            : base(startMs, endMs)
        {
        }

        public override bool Overlap(Window other)
            => other is TimeWindow && StartMs < ((TimeWindow)other).EndMs && ((TimeWindow)other).StartMs < EndMs;
    }
}
