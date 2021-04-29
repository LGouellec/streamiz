using NUnit.Framework;
using Streamiz.Kafka.Net.State.Helper;
using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class InMemoryWindowKeySchemaTests
    {
        [Test]
        public void ExtractStoreKeyBytesTest()
        {
            var bytes = new byte[10] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
            var expected = new byte[2] { 0, 1 };
            var r = State.Helper.InMemoryWindowKeySchema.ExtractStoreKeyBytes(bytes);
            Assert.AreEqual(expected, r);
        }

        [Test]
        public void ExtractStoreTimestamp()
        {
            string key = "test";
            long ts = DateTime.Now.GetMilliseconds();
            List<byte> bytes = new List<byte>();
            bytes.AddRange(Encoding.UTF8.GetBytes(key));
            bytes.AddRange(BitConverter.GetBytes(ts));
            long r = State.Helper.InMemoryWindowKeySchema.ExtractStoreTimestamp(bytes.ToArray());
            Assert.AreEqual(ts, r);
        }
    }
}
