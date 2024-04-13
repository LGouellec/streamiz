using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.State.Logging
{
    internal class ChangeLoggingTimestampedWindowBytesStore :
        ChangeLoggingWindowBytesStore
    {
        public ChangeLoggingTimestampedWindowBytesStore(IWindowStore<Bytes, byte[]> wrapped, bool retainDuplicates) 
            : base(wrapped, retainDuplicates)
        { }

        protected override void Publish(Bytes key, byte[] valueAndTs)
        {
            if (valueAndTs != null)
            {
                (long ts, byte[] data) = ValueAndTimestampSerDes.Extract(valueAndTs);
                context.Log(Name, key, data, ts);
            }
            else
            {
                context.Log(Name, key, null, context.RecordContext.Timestamp);
            }
        }
    }
}
