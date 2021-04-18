using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Enumerator;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal interface IKeySchema
    {
        Bytes UpperRange(Bytes key, long to);

        Bytes LowerRange(Bytes key, long from);

        Bytes UpperRangeFixedSize(Bytes key, long to);

        Bytes LowerRangeFixedSize(Bytes key, long from);

        long SegmentTimestamp(Bytes key);

        Func<IKeyValueEnumerator<Bytes, byte[]>, bool> HasNextCondition(Bytes binaryKeyFrom, Bytes binaryKeyTo, long from, long to);

        IList<S> SegmentsToSearch<S>(ISegments<S> segments, long from, long to, bool forward)
            where S : ISegment;
    }
}
