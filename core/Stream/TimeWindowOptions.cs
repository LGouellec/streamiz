using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream
{
    public class TimeWindowOptions : WindowOptions<TimeWindow>
    {
        public static readonly long DEFAULT_RETENTION_MS = 24 * 60 * 60 * 1000L; // one day

        private readonly long advanceMs;
        private readonly long graceMs;
        private readonly long maintainDurationMs;

        public TimeWindowOptions(long sizeMs, long advanceMs, long graceMs, long maintainDurationMs)
        {
            Size = sizeMs;
            this.advanceMs = advanceMs;
            this.graceMs = this.graceMs;
            this.maintainDurationMs = maintainDurationMs;
        }

        public override long Size { get; }

        public override long GracePeriodMs => graceMs != -1 ? graceMs : maintainDurationMs - Size;

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
            => new TimeWindowOptions(sizeMs, sizeMs, -1, DEFAULT_RETENTION_MS);

        public static TimeWindowOptions Of(TimeSpan size)
            => Of((long)size.TotalMilliseconds);
    }
}
