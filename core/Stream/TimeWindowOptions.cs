using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream
{
    public class TimeWindowOptions : WindowOptions<TimeWindow>
    {
        private readonly long advanceMs;

        public TimeWindowOptions(long sizeMs, long advanceMs)
        {
            Size = sizeMs;
            this.advanceMs = advanceMs;
            GracePeriodMs = sizeMs;
        }

        public override long Size { get; }

        public override long GracePeriodMs { get; }

        public override IDictionary<long, TimeWindow> WindowsFor(long timestamp)
        {
            long windowStart = (Math.Max(0, timestamp - Size + advanceMs) / advanceMs) * advanceMs;
            IDictionary<long, TimeWindow> windows = new Dictionary<long, TimeWindow>();
            while (windowStart <= timestamp)
            {
                TimeWindow window = new TimeWindow(windowStart, windowStart + Size);
                windows.Add(windowStart, window);
                windowStart += advanceMs;
            }
            return windows;
        }

        public static TimeWindowOptions Of(long sizeMs)
            => new TimeWindowOptions(sizeMs, sizeMs);

        public static TimeWindowOptions Of(TimeSpan size)
            => Of((long)size.TotalMilliseconds);
    }
}
