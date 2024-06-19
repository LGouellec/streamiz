using System;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Internal;

namespace Streamiz.Kafka.Net.State.Cache
{
    internal class SegmentedCacheFunction : ICacheFunction
    {
        private static int SEGMENT_ID_BYTES = 8;
        
        private IKeySchema keySchema;
        public long SegmentInterval { get; }

        public SegmentedCacheFunction(IKeySchema keySchema, long segmentInterval)
        {
            this.keySchema = keySchema;
            SegmentInterval = segmentInterval;
        }

        public Bytes Key(Bytes cacheKey) => Bytes.Wrap(BytesFromCacheKey(cacheKey));

        public Bytes CacheKey(Bytes cacheKey) => CacheKey(cacheKey, SegmentId(cacheKey));
        
        public Bytes CacheKey(Bytes key, long segmentId) {
            byte[] keyBytes = key.Get;
            using var buf = ByteBuffer.Build(SEGMENT_ID_BYTES + keyBytes.Length, true);
            buf.PutLong(segmentId).Put(keyBytes);
            return Bytes.Wrap(buf.ToArray());
        }
        
        public byte[] BytesFromCacheKey( Bytes cacheKey) {
            byte[] binaryKey = new byte[cacheKey.Get.Length - SEGMENT_ID_BYTES];
            Array.Copy(cacheKey.Get, SEGMENT_ID_BYTES, binaryKey, 0, binaryKey.Length);
            return binaryKey;
        }

        public virtual long SegmentId(Bytes key) {
            return SegmentId(keySchema.SegmentTimestamp(key));
        }
        
        public long SegmentId( long timestamp) {
            return timestamp / SegmentInterval;
        }

        public int CompareSegmentedKeys( Bytes cacheKey,  Bytes storeKey) {
             long storeSegmentId = SegmentId(storeKey);
             
             using var byteBuffer = ByteBuffer.Build(cacheKey.Get, true);
             long cacheSegmentId = byteBuffer.GetLong(0);

            int segmentCompare = cacheSegmentId.CompareTo(storeSegmentId);
            if (segmentCompare == 0) {
                 byte[] cacheKeyBytes = cacheKey.Get;
                 byte[] storeKeyBytes = storeKey.Get;
                 return BytesComparer.Compare(
                     cacheKeyBytes
                         .AsSpan(SEGMENT_ID_BYTES, cacheKeyBytes.Length - SEGMENT_ID_BYTES)
                         .ToArray(),
                     storeKeyBytes);
            }

            return segmentCompare;
        }
    }
}