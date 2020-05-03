using NUnit.Framework;
using Streamiz.Kafka.Net.Processors.Internal;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class RecordQueueTests
    {
        [Test]
        public void CheckMaximumSize()
        {
            RecordQueue<string> queue = new RecordQueue<string>(10, "", "test", null);
            Assert.AreEqual(10, queue.MaxSize);
        }

        [Test]
        public void CheckAddWhenMaximumSizeAttempt()
        {
            RecordQueue<string> queue = new RecordQueue<string>(10, "", "test", null);
            for (int i = 0; i < 10; ++i)
                queue.AddRecord($"test-{i}");
            Assert.AreEqual(10, queue.Size);
            bool b = queue.AddRecord($"test-too");
            Assert.AreEqual(10, queue.Size);
            Assert.IsFalse(b);
        }
    }
}
