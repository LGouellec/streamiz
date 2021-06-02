using NUnit.Framework;
using Streamiz.Kafka.Net.State.Helper;
using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class WindowKeySchemaTests
    {
        [Test]
        public void ExtractStoreKeyBytesTest()
        {
            var bytes = new byte[14] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 };
            var expected = new byte[2] { 0, 1 };
            var r = WindowKeyHelper.ExtractStoreKeyBytes(bytes);
            Assert.AreEqual(expected, r);
        }

        [Test]
        public void ExtractStoreTimestamp()
        {
            string key = "test";
            long ts = DateTime.Now.GetMilliseconds();
            int seq = 0;
            List<byte> bytes = new List<byte>();
            bytes.AddRange(Encoding.UTF8.GetBytes(key));
            bytes.AddRange(BitConverter.GetBytes(ts));
            bytes.AddRange(BitConverter.GetBytes(seq));
            long r = WindowKeyHelper.ExtractStoreTimestamp(bytes.ToArray());
            Assert.AreEqual(ts, r);
        }

        [Test]
        public void ExtractStoreSeqnum()
        {
            string key = "test";
            long ts = DateTime.Now.GetMilliseconds();
            int seq = 14;
            List<byte> bytes = new List<byte>();
            bytes.AddRange(Encoding.UTF8.GetBytes(key));
            bytes.AddRange(BitConverter.GetBytes(ts));
            bytes.AddRange(BitConverter.GetBytes(seq));
            long r = WindowKeyHelper.ExtractStoreSequence(bytes.ToArray());
            Assert.AreEqual(seq, r);
        }
    }
}
