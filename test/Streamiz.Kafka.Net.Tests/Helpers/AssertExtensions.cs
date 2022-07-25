using System;
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
    }
}