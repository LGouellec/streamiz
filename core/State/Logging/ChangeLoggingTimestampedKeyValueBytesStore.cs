using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;
using System.Collections.Generic;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.State.Logging
{
    internal class ChangeLoggingTimestampedKeyValueBytesStore : ChangeLoggingKeyValueBytesStore
    {
        public ChangeLoggingTimestampedKeyValueBytesStore(IKeyValueStore<Bytes, byte[]> wrapped)
            : base(wrapped)
        {
        }

        protected override void Publish(Bytes key, byte[] value)
        {
            if(value == null)
                context.Log(Name, key, null, context.RecordContext.Timestamp);
            else
            {
                (long ts, byte[] data) = ValueAndTimestampSerDes.Extract(value);
                context.Log(Name, key, data, ts);
            }
        }
    }
}