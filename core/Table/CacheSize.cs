namespace Streamiz.Kafka.Net.Table
{
    /// <summary>
    /// Specify the maximum cache size
    /// </summary>
    public class CacheSize
    {
        /// <summary>
        /// Number of binary bytes
        /// </summary>
        public long CacheSizeBytes { get; private set; }
        
        /// <summary>
        /// Create a <see cref="CacheSize"/> based on the <paramref name="bytes"/> paramters.
        /// </summary>
        /// <param name="bytes">Number of bytes (in binary) of your cache size</param>
        /// <returns></returns>
        public static CacheSize OfB(int bytes) => new CacheSize { CacheSizeBytes = bytes };
        
        /// <summary>
        /// Create a <see cref="CacheSize"/> based on the <paramref name="kilobytes"/> paramters.
        /// </summary>
        /// <param name="kilobytes">Number of kilobytes (in binary) of your cache size</param>
        /// <returns></returns>
        public static CacheSize OfKb(int kilobytes) => new CacheSize { CacheSizeBytes = kilobytes * 1024 };
        
        /// <summary>
        /// Create a <see cref="CacheSize"/> based on the <paramref name="megabytes"/> paramters.
        /// </summary>
        /// <param name="megabytes">Number of megabytes (in binary) of your cache size</param>
        /// <returns></returns>
        public static CacheSize OfMb(int megabytes) => new CacheSize { CacheSizeBytes = megabytes * 1024 * 1024 };
        
        /// <summary>
        /// Create a <see cref="CacheSize"/> based on the <paramref name="gigabytes"/> paramters.
        /// </summary>
        /// <param name="gigabytes">Number of gigabytes (in binary) of your cache size</param>
        /// <returns></returns>
        public static CacheSize OfGb(int gigabytes) => new CacheSize { CacheSizeBytes = gigabytes * 1024 * 1024 * 1024 };
    }
}