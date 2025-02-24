using Streamiz.Kafka.Net.Metrics.Internal;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class RocksDbKeyValueSegments : AbstractSegments<RocksDbKeyValueSegment>
    {
        private readonly RocksDbMetricsRecorder metricsRecorder;
        
        public RocksDbKeyValueSegments(string name, long retention, long segmentInterval, string metricsScope)
            : base(name, retention, segmentInterval)
        {
            metricsRecorder = new RocksDbMetricsRecorder(metricsScope, name);
        }

        public override RocksDbKeyValueSegment GetOrCreateSegment(long segmentId, ProcessorContext context)
        {
            if (segments.TryGetValue(segmentId, out var createSegment))
                return createSegment;
            else
            {
                var segment = new RocksDbKeyValueSegment(
                    SegmentName(segmentId),
                    name,
                    segmentId,
                    metricsRecorder);

                segments.Add(segmentId, segment);
                segment.OpenDB(context);
                return segment;
            }
        }

        public override void OpenExisting(ProcessorContext context, long streamTime)
        {
            metricsRecorder.Init(context.Metrics, context.Id);
            base.OpenExisting(context, streamTime);
        }
    }
}