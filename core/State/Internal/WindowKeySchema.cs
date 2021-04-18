using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Enumerator;
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
        public static Bytes toStoreKeyBinary(Bytes key,
                                          long timestamp,
                                          int seqnum)
        {
            byte[] serializedKey = key.Get;
            return toStoreKeyBinary(serializedKey, timestamp, seqnum);
        }

        public static Bytes toStoreKeyBinary<K>(K key,
                                                  long timestamp,
                                                  int seqnum,
                                                  ISerDes<K> keySerdes)
        {
            byte[] serializedKey = keySerdes.Serialize(key, new Confluent.Kafka.SerializationContext());
            return toStoreKeyBinary(serializedKey, timestamp, seqnum);
        }

        public static Bytes toStoreKeyBinary(Windowed<Bytes> timeKey,
                                              int seqnum)
        {
            byte[] bytes = timeKey.Key.Get;
            return toStoreKeyBinary(bytes, timeKey.Window.StartMs, seqnum);
        }

        public static Bytes toStoreKeyBinary<K>(Windowed<K> timeKey,
                                                  int seqnum,
                                                   ISerDes<K> keySerdes)
        {
            byte[] serializedKey = keySerdes.Serialize(timeKey.Key, new Confluent.Kafka.SerializationContext());
            return toStoreKeyBinary(serializedKey, timeKey.Window.StartMs, seqnum);
        }

        // package private for testing
        static Bytes toStoreKeyBinary(byte[] serializedKey,
                                       long timestamp,
                                       int seqnum)
        {
            ByteBuffer buf = ByteBuffer.allocate(serializedKey.length + TIMESTAMP_SIZE + SEQNUM_SIZE);
            buf.put(serializedKey);
            buf.putLong(timestamp);
            buf.putInt(seqnum);

            return Bytes.wrap(buf.array());
        }

        static byte[] extractStoreKeyBytes(byte[] binaryKey)
        {
            byte[] bytes = new byte[binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE];
            System.arraycopy(binaryKey, 0, bytes, 0, bytes.length);
            return bytes;
        }

        static <K> K extractStoreKey(byte[] binaryKey,
                                      StateSerdes<K, ?> serdes)
        {
            byte[] bytes = new byte[binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE];
            System.arraycopy(binaryKey, 0, bytes, 0, bytes.length);
            return serdes.keyFrom(bytes);
        }

        static long extractStoreTimestamp(byte[] binaryKey)
        {
            return ByteBuffer.wrap(binaryKey).getLong(binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE);
        }

        static int extractStoreSequence(byte[] binaryKey)
        {
            return ByteBuffer.wrap(binaryKey).getInt(binaryKey.length - SEQNUM_SIZE);
        }

        public static <K> Windowed<K> fromStoreKey(byte[] binaryKey,
                                                    long windowSize,
                                                    Deserializer<K> deserializer,
                                                    String topic)
        {
            K key = deserializer.deserialize(topic, extractStoreKeyBytes(binaryKey));
            Window window = extractStoreWindow(binaryKey, windowSize);
            return new Windowed<>(key, window);
        }

        public static <K> Windowed<K> fromStoreKey(Windowed<Bytes> windowedKey,
                                                    Deserializer<K> deserializer,
                                                    String topic)
        {
            K key = deserializer.deserialize(topic, windowedKey.key().get());
            return new Windowed<>(key, windowedKey.window());
        }

        public static Windowed<Bytes> fromStoreBytesKey(byte[] binaryKey,
                                                         long windowSize)
        {
            Bytes key = Bytes.wrap(extractStoreKeyBytes(binaryKey));
            Window window = extractStoreWindow(binaryKey, windowSize);
            return new Windowed<>(key, window);
        }

        static Window extractStoreWindow(byte[] binaryKey,
                                          long windowSize)
        {
            ByteBuffer buffer = ByteBuffer.wrap(binaryKey);
            long start = buffer.getLong(binaryKey.length - TIMESTAMP_SIZE - SEQNUM_SIZE);
            return timeWindowForSize(start, windowSize);
        }
        #endregion
    }
}
