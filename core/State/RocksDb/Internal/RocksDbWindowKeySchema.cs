using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.RocksDb.Internal
{
    /// <summary>
    /// Like JAVA Implementation, no advantages to rewrite
    /// </summary>
    internal class RocksDbWindowKeySchema : IKeySchema
    {
        private const int SEQNUM_SIZE = 4;
        private const int TIMESTAMP_SIZE = 8;
        private const int SUFFIX_SIZE = TIMESTAMP_SIZE + SEQNUM_SIZE;
        private static readonly byte[] MIN_SUFFIX = new byte[SUFFIX_SIZE];
        private static readonly IComparer<Bytes> bytesComparer = new BytesComparer();

        public Func<IKeyValueEnumerator<Bytes, byte[]>, bool> HasNextCondition(Bytes binaryKeyFrom, Bytes binaryKeyTo, long from, long to)
        {
            return (enumerator) =>
            {
                while (enumerator.MoveNext())
                {
                    var bytes = enumerator.PeekNextKey();
                    Bytes keyBytes = Bytes.Wrap(ExtractStoreKeyBytes(bytes.Get));
                    long time = ExtractStoreTimestamp(bytes.Get);
                    if ((binaryKeyFrom == null || bytesComparer.Compare(keyBytes, binaryKeyFrom) >= 0)
                        && (binaryKeyTo == null || bytesComparer.Compare(keyBytes, binaryKeyTo) <= 0)
                        && time >= from
                        && time <= to)
                        return true;
                        
                }
                return false;
            };
        }

        public Bytes LowerRange(Bytes key, long from)
            => OrderedBytes.LowerRange(key, MIN_SUFFIX);

        public Bytes LowerRangeFixedSize(Bytes key, long from)
            => ToStoreKeyBinary(key, Math.Max(0, from), 0);

        public IList<S> SegmentsToSearch<S>(ISegments<S> segments, long from, long to, bool forward) where S : ISegment
            => segments.Segments(from, to, forward).ToList();

        public long SegmentTimestamp(Bytes key)
            => ExtractStoreTimestamp(key.Get);

        public Bytes UpperRange(Bytes key, long to)
        {
            byte[] maxSuffix = ByteBuffer.Build(SUFFIX_SIZE)
            .PutLong(to)
            .PutInt(int.MaxValue)
            .ToArray();

            return OrderedBytes.UpperRange(key, maxSuffix);
        }

        public Bytes UpperRangeFixedSize(Bytes key, long to)
            => ToStoreKeyBinary(key, to, Int32.MaxValue);

        #region Static

        public static TimeWindow TimeWindowForSize(long startMs, long windowSize)
        {
            long endMs = startMs + windowSize;

            if (endMs < 0)
            {
                endMs = long.MaxValue;
            }

            return new TimeWindow(startMs, endMs);
        }

        public static byte[] ToBinary<K>(Windowed<K> timeKey, ISerDes<K> serializer, String topic)
        {
            byte[] bytes = serializer.Serialize(timeKey.Key, new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Key, topic));
            ByteBuffer buf = ByteBuffer.Build(bytes.Length + TIMESTAMP_SIZE);
            buf.Put(bytes);
            buf.PutLong(timeKey.Window.StartMs);
            return buf.ToArray();
        }

        public static Windowed<K> From<K>(byte[] binaryKey, long windowSize, ISerDes<K> deserializer, String topic)
        {
            binaryKey = binaryKey ?? new byte[0];
            byte[] bytes = binaryKey.AsSpan(0, binaryKey.Length - TIMESTAMP_SIZE).ToArray();
            K key = deserializer.Deserialize(bytes, new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Key, topic));
            Window window = ExtractWindow(binaryKey, windowSize);
            return new Windowed<K>(key, window);
        }

        private static Window ExtractWindow( byte[] binaryKey, long windowSize)
        {
            binaryKey = binaryKey ?? new byte[0];
            ByteBuffer buffer = ByteBuffer.Build(binaryKey);
             long start = buffer.GetLong(binaryKey.Length - TIMESTAMP_SIZE);
            return TimeWindowForSize(start, windowSize);
        }

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

        public static Bytes ToStoreKeyBinary(byte[] serializedKey, long timestamp, int seqnum)
        {
            serializedKey = serializedKey ?? new byte[0];
            ByteBuffer buf = ByteBuffer.Build(serializedKey.Length + TIMESTAMP_SIZE + SEQNUM_SIZE);
            buf.Put(serializedKey);
            buf.PutLong(timestamp);
            buf.PutInt(seqnum);

            return Bytes.Wrap(buf.ToArray());
        }

        public static byte[] ExtractStoreKeyBytes(byte[] binaryKey)
        {
            binaryKey = binaryKey ?? new byte[0];
            return binaryKey.AsSpan(0, binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE).ToArray();
        }

        public static K ExtractStoreKey<K>(byte[] binaryKey, ISerDes<K> keySerdes)
        {
            binaryKey = binaryKey ?? new byte[0];
            byte[] bytes = binaryKey.AsSpan(0, binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE).ToArray();
            return keySerdes.Deserialize(bytes, new Confluent.Kafka.SerializationContext());
        }

        public static long ExtractStoreTimestamp(byte[] binaryKey)
        {
            binaryKey = binaryKey ?? new byte[0];
            return ByteBuffer.Build(binaryKey).GetLong(binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE);
        }

        public static int ExtractStoreSequence(byte[] binaryKey)
        {
            binaryKey = binaryKey ?? new byte[0];
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

        public static Window ExtractStoreWindow(byte[] binaryKey, long windowSize)
        {
            ByteBuffer buffer = ByteBuffer.Build(binaryKey);
            long start = buffer.GetLong(binaryKey.Length - TIMESTAMP_SIZE - SEQNUM_SIZE);
            return TimeWindowForSize(start, windowSize);
        }
        
        #endregion
    }
}