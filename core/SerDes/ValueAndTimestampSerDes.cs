using kafka_stream_core.State;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace kafka_stream_core.SerDes
{
    internal class ValueAndTimestampSerDes<V> : AbstractSerDes<ValueAndTimestamp<V>>
    {
        private readonly ISerDes<V> innerSerdes;

        public ValueAndTimestampSerDes(ISerDes<V> innerSerdes)
        {
            this.innerSerdes = innerSerdes;
        }

        public override ValueAndTimestamp<V> Deserialize(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            using (var reader = new BinaryReader(stream))
            {
                long t = reader.ReadInt64();
                int length = reader.ReadInt32();
                byte[] d = reader.ReadBytes(length);
                V v = innerSerdes.Deserialize(d);
                ValueAndTimestamp<V> obj = new ValueAndTimestamp<V>(t, v);
                return obj;
            }
        }

        public override byte[] Serialize(ValueAndTimestamp<V> data)
        {
            using(var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                byte[] innerobj = innerSerdes.Serialize(data.Value);

                writer.Write(data.Timestamp);
                writer.Write(innerobj.Length);
                writer.Write(innerobj);
                writer.Flush();
                return stream.ToArray();
            }
        }
    }
}
