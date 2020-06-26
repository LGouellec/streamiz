using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Stream
{
    public class TumblingWindowOptions : TimeWindowOptions
    {
        protected TumblingWindowOptions(long sizeMs, long advanceMs, long graceMs, long maintainDurationMs) 
            : base(sizeMs, advanceMs, graceMs, maintainDurationMs)
        {
        }

        public static TumblingWindowOptions Of(long sizeMs)
            => new TumblingWindowOptions(sizeMs, sizeMs, -1, DEFAULT_RETENTION_MS);

        public static TumblingWindowOptions Of(TimeSpan size)
            => Of((long)size.TotalMilliseconds);

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
    }
}
