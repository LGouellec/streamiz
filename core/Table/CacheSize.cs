namespace Streamiz.Kafka.Net.Table
{
    public class CacheSize
    {
        public long CacheSizeBytes { get; private set; }
        
        public static CacheSize OfB(int bytes) => new CacheSize { CacheSizeBytes = bytes };
        public static CacheSize OfKb(int kilobytes) => new CacheSize { CacheSizeBytes = kilobytes * 1024 };
        public static CacheSize OfMb(int megabytes) => new CacheSize { CacheSizeBytes = megabytes * 1024 * 1024 };
        public static CacheSize OfGb(int gigabytes) => new CacheSize { CacheSizeBytes = gigabytes * 1024 * 1024 * 1024 };
    }
}