using System;
using System.Linq;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.State.Suppress.Internal
{
    internal class BufferValue
    {
        private const int NULL_VALUE = -1;
        private const int OLD_PREV_DUPLICATE = -2;
        
        internal IRecordContext RecordContext { get; }
        internal byte[] PriorValue { get; }
        internal byte[] OldValue{ get; }
        internal byte[] NewValue{ get; }

        internal BufferValue(byte[] priorValue, byte[] oldValue, byte[] newValue, IRecordContext recordContext)
        {
            OldValue = oldValue;
            NewValue = newValue;
            RecordContext = recordContext;

            if (ReferenceEquals(priorValue, oldValue))
                this.PriorValue = oldValue;
            else
                this.PriorValue = priorValue;
        }

        public override bool Equals(object obj)
        {
            return obj is BufferValue value &&
                   ((PriorValue == null && value.PriorValue == null) || PriorValue.SequenceEqual(value.PriorValue)) &&
                   ((OldValue == null && value.OldValue == null) || OldValue.SequenceEqual(value.OldValue)) &&
                       ((NewValue == null && value.NewValue == null) || NewValue.SequenceEqual(value.NewValue));
        }

        public long MemoryEstimatedSize()
        {
            return (PriorValue?.Length ?? 0) +
                   (OldValue == null || PriorValue == OldValue ? 0 : OldValue.Length) +
                   (NewValue?.Length ?? 0) +
                   RecordContext.MemorySizeEstimate;
        }

        public ByteBuffer Serialize(int endPadding)
        {
            void AddValue(ByteBuffer buffer, byte[] value)
            {
                if (value == null) {
                    buffer.PutInt(NULL_VALUE);
                } else {
                    buffer.PutInt(value.Length);
                    buffer.Put(value);
                }
            }
            
            int sizeOfValueLength = sizeof(Int32);

            int sizeOfPriorValue = PriorValue?.Length ?? 0;
            int sizeOfOldValue = OldValue == null || PriorValue == OldValue ? 0 : OldValue.Length;
            int sizeOfNewValue = NewValue?.Length ?? 0;

            byte[] serializedContext = RecordContext.Serialize();
            
            var buffer = ByteBuffer.Build(
                serializedContext.Length +
                sizeOfValueLength + sizeOfPriorValue +
                sizeOfValueLength + sizeOfOldValue +
                sizeOfValueLength + sizeOfNewValue +
                endPadding, true);

            buffer.Put(serializedContext);
            AddValue(buffer, PriorValue);

            if (OldValue == null)
                buffer.PutInt(NULL_VALUE);
            else if (ReferenceEquals(PriorValue, OldValue))
                buffer.PutInt(OLD_PREV_DUPLICATE);
            else
            {
                buffer.PutInt(OldValue.Length);
                buffer.Put(OldValue);
            }
            
            AddValue(buffer, NewValue);

            return buffer;
        }

        public static BufferValue Deserialize(ByteBuffer buffer)
        {
            var context = Processors.Internal.RecordContext.Deserialize(buffer);
            var priorValue = buffer.GetNullableSizePrefixedArray();
            byte[] oldValue;
            
            var oldValueLength = buffer.GetInt();
            if (oldValueLength == OLD_PREV_DUPLICATE)
                oldValue = priorValue;
            else if (oldValueLength == NULL_VALUE)
                oldValue = null;
            else
                oldValue = buffer.GetBytes(oldValueLength);

            var newValue = buffer.GetNullableSizePrefixedArray();
            
            return new BufferValue(priorValue, oldValue, newValue, context);
        }

    }
}