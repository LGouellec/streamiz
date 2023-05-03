using NUnit.Framework;
using Streamiz.Kafka.Net.State.Cache;

namespace Streamiz.Kafka.Net.Tests
{
    public class TestCache
    {
        [Test]
        public void test()
        {
            LRUCache cache = new LRUCache();
            cache.test();
        }
    }
}