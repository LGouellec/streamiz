using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Stream
{
    public class TimeWindow : Window
    {
        public TimeWindow(long startMs, long endMs) 
            : base(startMs, endMs)
        {
        }

        public override bool Overlap(Window other)
        {
            throw new NotImplementedException();
        }
    }
}
