using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Internal;
using System;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class OrderedBytesTest
    {
        [Test]
        public void test()
        {
            BytesComparer comparer = new BytesComparer();
            var serdes = new StringSerDes();
            var bytes = serdes.Serialize("test", new Confluent.Kafka.SerializationContext());
            long to = DateTime.Now.GetMilliseconds();

            byte[] maxSuffix = ByteBuffer.Build(12)
                .PutLong(to)
                .PutInt(int.MaxValue)
                .ToArray();

            var bytes2 = OrderedBytes.UpperRange(Bytes.Wrap(bytes), maxSuffix);
            var bytes3 = OrderedBytes.LowerRange(Bytes.Wrap(bytes), new byte[12]);
            int r = comparer.Compare(bytes2, bytes3);
        }
    }
}