using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Cache
{
    internal class ThreadLRUCache
    {
        private readonly ILog logger;
        private readonly long maxCacheSizeElements;
        private readonly IDictionary<string, LRUCache> caches = new Dictionary<string, LRUCache>();

        public ThreadLRUCache(string prefix, long maxCacheSizeElements)
        {
            this.maxCacheSizeElements = maxCacheSizeElements;
            logger = Logger.GetLogger(typeof(ThreadLRUCache));
        }

        public void Flush(string @namespace)
        {
        }

        // public void 
    }
}
