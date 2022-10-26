using System;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.SerDes.Internal
{
    internal class ChangeSerDes<V> : AbstractSerDes<Change<V>>
    {
        private ISerDes<V> innerSerdes;
        private static readonly int SIZE_INT32 = 4;

        public ChangeSerDes(ISerDes<V> innerSerdes)
        {
            this.innerSerdes = innerSerdes;
        }

        private int CalculateCapacity(byte[] old, byte[] @new) =>
            SIZE_INT32 + (old?.Length ?? 0) + SIZE_INT32 + (@new?.Length ?? 0);

        public override byte[] Serialize(Change<V> data, SerializationContext context)
        {
            byte[] oldValueBytes = innerSerdes.Serialize(data.OldValue, context);
            byte[] newValueBytes = innerSerdes.Serialize(data.NewValue, context);

            using var buffer = ByteBuffer.Build(CalculateCapacity(oldValueBytes, newValueBytes));
            return buffer
                .PutInt(oldValueBytes?.Length ?? 0)
                .Put(oldValueBytes)
                .PutInt(newValueBytes?.Length ?? 0)
                .Put(newValueBytes)
                .ToArray();
        }

        public override Change<V> Deserialize(byte[] data, SerializationContext context)
        {
            using var byteBuffer = ByteBuffer.Build(data);
            {
                var oldValueLength = byteBuffer.GetInt(0);
                var oldBytes = oldValueLength > 0 ? byteBuffer.GetBytes(SIZE_INT32, oldValueLength) : null;
                var newValueLength = byteBuffer.GetInt(SIZE_INT32 + oldValueLength);
                var newBytes = newValueLength > 0
                    ? byteBuffer.GetBytes(SIZE_INT32 + oldValueLength + SIZE_INT32, newValueLength)
                    : null;
                
                return new Change<V>(
                    innerSerdes.Deserialize(oldBytes, context),
                    innerSerdes.Deserialize(newBytes, context));
            }
        }

        public override void Initialize(SerDesContext context)
        {
            if (innerSerdes == null && context.Config.DefaultValueSerDes is ISerDes<V>)
                innerSerdes = (ISerDes<V>) context.Config.DefaultValueSerDes;
            
            if(innerSerdes == null)
                throw new StreamsException($"ChangeSerdes<{typeof(V)}> is not correctly configured. Please set explicitly serdes in previous operation.");
            
            innerSerdes.Initialize(context);
        }
    }
}