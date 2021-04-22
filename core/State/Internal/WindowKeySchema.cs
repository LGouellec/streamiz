using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.State.Internal
{
    /// <summary>
    /// Like JAVA Implementation, no advantages to rewrite
    /// </summary>
    internal class WindowKeySchema : IKeySchema
    {
        private const int SEQNUM_SIZE = 4;
        private const int TIMESTAMP_SIZE = 8;
        private const int SUFFIX_SIZE = TIMESTAMP_SIZE + SEQNUM_SIZE;
        private static readonly byte[] MIN_SUFFIX = new byte[SUFFIX_SIZE];

        public Func<IKeyValueEnumerator<Bytes, byte[]>, bool> HasNextCondition(Bytes binaryKeyFrom, Bytes binaryKeyTo, long from, long to)
        {
            throw new NotImplementedException();
        }

        public Bytes LowerRange(Bytes key, long from)
        {
            throw new NotImplementedException();
        }

        public Bytes LowerRangeFixedSize(Bytes key, long from)
        {
            throw new NotImplementedException();
        }

        public IList<S> SegmentsToSearch<S>(ISegments<S> segments, long from, long to, bool forward) where S : ISegment
        {
            throw new NotImplementedException();
        }

        public long SegmentTimestamp(Bytes key)
        {
            throw new NotImplementedException();
        }

        public Bytes UpperRange(Bytes key, long to)
        {
            throw new NotImplementedException();
        }

        public Bytes UpperRangeFixedSize(Bytes key, long to)
        {
            throw new NotImplementedException();
        }

        #region Internal
        public static Bytes ToStoreKeyBinary(Bytes key, long timestamp, int seqnum)
        {
            byte[] serializedKey = key.Get;
            return ToStoreKeyBinary(serializedKey, timestamp, seqnum);
        }

        public static Bytes ToStoreKeyBinary<K>(K key, long timestamp, int seqnum, ISerDes<K> keySerdes)
        {
            byte[] serializedKey = keySerdes.Serialize(key, new Confluent.Kafka.SerializationContext());
            return ToStoreKeyBinary(serializedKey, timestamp, seqnum);
        }

        public static Bytes ToStoreKeyBinary(Windowed<Bytes> timeKey, int seqnum)
        {
            byte[] bytes = timeKey.Key.Get;
            return ToStoreKeyBinary(bytes, timeKey.Window.StartMs, seqnum);
        }

        public static Bytes ToStoreKeyBinary<K>(Windowed<K> timeKey, int seqnum, ISerDes<K> keySerdes)
        {
            byte[] serializedKey = keySerdes.Serialize(timeKey.Key, new Confluent.Kafka.SerializationContext());
            return ToStoreKeyBinary(serializedKey, timeKey.Window.StartMs, seqnum);
        }

        // package private for testing
        static Bytes ToStoreKeyBinary(byte[] serializedKey, long timestamp, int seqnum)
        {
            ByteBuffer buf = ByteBuffer.Build(serializedKey.Length + TIMESTAMP_SIZE + SEQNUM_SIZE);
            buf.Put(serializedKey);
            buf.PutLong(timestamp);
            buf.PutInt(seqnum);

            return Bytes.Wrap(buf.ToArray());
        }

        static byte[] ExtractStoreKeyBytes(byte[] binaryKey)
        {
            return binaryKey.AsSpan(0, binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE).ToArray();
        }

        static K ExtractStoreKey<K>(byte[] binaryKey, ISerDes<K> keySerdes)
        {
            byte[] bytes = binaryKey.AsSpan(0, binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE).ToArray();
            return keySerdes.Deserialize(bytes, new Confluent.Kafka.SerializationContext());
        }

        static long ExtractStoreTimestamp(byte[] binaryKey)
        {
            return ByteBuffer.Build(binaryKey).GetLong(binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE);
        }

        static int ExtractStoreSequence(byte[] binaryKey)
        {
            return ByteBuffer.Build(binaryKey).GetInt(binaryKey.Length - SEQNUM_SIZE);
        }

        public static Windowed<K> FromStoreKey<K>(byte[] binaryKey, long windowSize, ISerDes<K> keySerdes, String topic)
        {
            K key = keySerdes.Deserialize(ExtractStoreKeyBytes(binaryKey), new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Key, topic));
            Window window = ExtractStoreWindow(binaryKey, windowSize);
            return new Windowed<K>(key, window);
        }

        public static Windowed<K> FromStoreKey<K>(Windowed<Bytes> windowedKey, ISerDes<K> keySerdes, String topic)
        {
            K key = keySerdes.Deserialize(windowedKey.Key.Get, new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Key, topic));
            return new Windowed<K>(key, windowedKey.Window);
        }

        public static Windowed<Bytes> FromStoreBytesKey(byte[] binaryKey, long windowSize)
        {
            Bytes key = Bytes.Wrap(ExtractStoreKeyBytes(binaryKey));
            Window window = ExtractStoreWindow(binaryKey, windowSize);
            return new Windowed<Bytes>(key, window);
        }

        static Window ExtractStoreWindow(byte[] binaryKey, long windowSize)
        {
            ByteBuffer buffer = ByteBuffer.Build(binaryKey);
            long start = buffer.GetLong(binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE);
            return TimeWindowForSize(start, windowSize);
        }
        
        #endregion
    }
}
