using Confluent.Kafka;
using NUnit.Framework;

namespace Streamiz.Kafka.Net.Tests.Helpers
{
    public static class AssertExtensions
    {
        public static void MessageEqual<K, V>((K, V) expected, ConsumeResult<K, V> actual)
        {
            Assert.AreEqual(expected.Item1, actual.Message.Key);
            Assert.AreEqual(expected.Item2, actual.Message.Value);
        }
    }
}