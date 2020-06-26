using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream
{
    public abstract class TimeWindowOptions : WindowOptions<TimeWindow>
    {
        public static readonly long DEFAULT_RETENTION_MS = 24 * 60 * 60 * 1000L; // one day

        protected readonly long advanceMs;
        protected readonly long graceMs;
        protected readonly long maintainDurationMs;

        protected TimeWindowOptions(long sizeMs, long advanceMs, long graceMs, long maintainDurationMs)
        {
            Size = sizeMs;
            this.advanceMs = advanceMs;
            this.graceMs = graceMs;
            this.maintainDurationMs = maintainDurationMs;
        }

        public override long Size { get; }

        public override long GracePeriodMs => graceMs != -1 ? graceMs : maintainDurationMs - Size;
    }
}
