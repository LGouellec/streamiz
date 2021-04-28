using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Crosscutting
{
    internal class LongComparer : IComparer<long>
    {
        public int Compare(long x, long y)
            => x.CompareTo(y);
    }
}
