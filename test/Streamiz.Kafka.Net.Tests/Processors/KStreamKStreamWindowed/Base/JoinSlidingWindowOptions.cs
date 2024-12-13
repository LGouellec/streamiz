using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    internal class JoinSlidingWindowOptions : JoinWindowOptions
    {
        public JoinSlidingWindowOptions(long beforeMs, long afterMs, long graceMs, long maintainDurationMs)
            : base(beforeMs, afterMs, graceMs, maintainDurationMs)
        {
        }
    }
}