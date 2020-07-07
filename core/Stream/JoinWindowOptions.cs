using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream
{
    public class JoinWindowOptions : WindowOptions<Window>
    {
        private readonly long beforeMs;
        private readonly long afterMs;
        private readonly long graceMs;
        private readonly long maintainDurationMs;

        protected JoinWindowOptions(long beforeMs, long afterMs, long graceMs, long maintainDurationMs)
        {
            this.beforeMs = beforeMs;
            this.afterMs = afterMs;
            this.graceMs = graceMs;
            this.maintainDurationMs = Math.Max(maintainDurationMs, Size);
        }

        public override long Size => beforeMs + afterMs;

        public override long GracePeriodMs => graceMs != -1 ? graceMs : maintainDurationMs - Size;

        public override IDictionary<long, Window> WindowsFor(long timestamp)
        {
            throw new NotImplementedException("WindowsFor() is not supported by JoinWindows");
        }

        #region Static

        #endregion
    }
}
