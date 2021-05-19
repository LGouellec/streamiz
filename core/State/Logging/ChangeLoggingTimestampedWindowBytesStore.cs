using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.State.Logging
{
    internal class ChangeLoggingTimestampedWindowBytesStore :
        ChangeLoggingWindowBytesStore
    {
        public ChangeLoggingTimestampedWindowBytesStore(IWindowStore<Bytes, byte[]> wrapped) 
            : base(wrapped)
        {
        }

        protected override void Publish(Bytes key, byte[] valueAndTs)
        {
            if (valueAndTs != null)
            {
                var data = ValueAndTimestampSerDes.Extract(valueAndTs);
                context.Log(Name, key, data.Item2, data.Item1);
            }
            else
            {
                context.Log(Name, key, null, context.RecordContext.Timestamp);
            }
        }
    }
}
