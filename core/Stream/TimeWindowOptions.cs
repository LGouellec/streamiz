using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Stream
{
    public class TimeWindowOptions : WindowOptions<TimeWindow>
    {
        public override long Size => throw new NotImplementedException();

        public override long GracePeriodMs => throw new NotImplementedException();

        public override IDictionary<long, TimeWindow> WindowsFor(long timestamp)
        {
            throw new NotImplementedException();
        }
    }
}
