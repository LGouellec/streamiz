using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.State.Cache
{
    internal interface ICacheFunction
    {
        byte[] BytesFromCacheKey(Bytes cacheKey);
        Bytes Key(Bytes cacheKey);
        Bytes CacheKey(Bytes cacheKey);
        Bytes CacheKey(Bytes key, long segmentId);
        long SegmentId(Bytes key);
        long SegmentId(long timestamp);
        long SegmentInterval { get; }
        int CompareSegmentedKeys(Bytes cacheKey, Bytes storeKey);
    }
}