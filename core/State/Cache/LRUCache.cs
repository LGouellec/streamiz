using System;
using System.Text;
using Microsoft.Extensions.Caching.Memory;

namespace Streamiz.Kafka.Net.State.Cache
{
    internal class LRUCache
    {
        private int numEvicted = 0;
        public void test()
        {
            numEvicted = 0;

            MemoryCache cache = new MemoryCache(new MemoryCacheOptions
            {
                SizeLimit = 1048576 
            });
            
            int i = 0;
            long totalBytes = 0;
            for (i = 0; i < Int32.MaxValue; ++i)
            {
                var cacheEntryOptions = new MemoryCacheEntryOptions()
                    .SetSize(Encoding.UTF8.GetBytes($"key{i}value").Length)
                    .RegisterPostEvictionCallback(removedCacheCb);

                cache.Set($"key{i}", "value", cacheEntryOptions);
                
                totalBytes += Encoding.UTF8.GetBytes($"key{i}value").Length;
                if (numEvicted >= 10)
                    break;
            }
            
            Console.WriteLine(numEvicted);
            Console.WriteLine(i);
            Console.WriteLine(totalBytes);
            cache.Dispose();
        }

        private void removedCacheCb(object key, object value, EvictionReason reason, object state)
        {
            Console.WriteLine($"Removed cache entry ({key}{value}) reason : {reason}");
            ++numEvicted;
        }
    }
}