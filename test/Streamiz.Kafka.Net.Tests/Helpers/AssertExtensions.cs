using System;
using System.Collections.Generic;
using System.Threading;
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
        
        public static void WaitUntil(Func<bool> condition, TimeSpan timeout, TimeSpan step)
        {
            DateTime start = DateTime.Now;
            while (!condition())
            {
                if (start.Add(timeout) < DateTime.Now)
                    return;
                Thread.Sleep((int)step.TotalMilliseconds);
            }
        }
        
        public static void VerifyKeyValueList<K, V>(List<KeyValuePair<K, V>> expected, List<KeyValuePair<K, V>> actual) {
            Assert.AreEqual(expected.Count, actual.Count);
            for (int i = 0; i < actual.Count; i++) {
                KeyValuePair<K, V> expectedKv = expected[i];
                KeyValuePair<K, V> actualKv = actual[i];
                Assert.AreEqual(expectedKv.Key, actualKv.Key);
                Assert.AreEqual(expectedKv.Value, actualKv.Value);
            }
        }
    }
}