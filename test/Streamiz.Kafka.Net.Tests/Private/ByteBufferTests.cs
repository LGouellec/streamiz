using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class ByteBufferTests
    {
        [Test]
        public void ByteBufferReadOperation()
        {
            long l = 126700;
            int i = 965;
            var longBytes = BitConverter.GetBytes(l);
            var intBytes = BitConverter.GetBytes(i);

            if (BitConverter.IsLittleEndian)
            {
                longBytes = longBytes.Reverse().ToArray();
                intBytes = intBytes.Reverse().ToArray();
            }

            var readArray = longBytes.Concat(intBytes).ToArray();
            var buffer = ByteBuffer.Build(readArray);

            long l1 = buffer.GetLong(0);
            int i1 = buffer.GetInt(sizeof(long));
            Assert.AreEqual(l, l1);
            Assert.AreEqual(i, i1);
            Assert.IsTrue(readArray.SequenceEqual(buffer.ToArray()));
            buffer.Dispose();
        }

        [Test]
        public void ByteBufferWriteOperation()
        {
            long l = 126700;
            int i = 965;
            byte[] array = new byte[] {1, 1, 1, 2};

            var longBytes = BitConverter.GetBytes(l);
            var intBytes = BitConverter.GetBytes(i);
            
            if (BitConverter.IsLittleEndian)
            {
                longBytes = longBytes.Reverse().ToArray();
                intBytes = intBytes.Reverse().ToArray();
            }

            var totalArray = longBytes.Concat(intBytes).Concat(array).ToArray();

            var buffer = ByteBuffer.Build(16);
            buffer.PutLong(l);
            buffer.PutInt(i);
            buffer.Put(array);

            Assert.IsTrue(totalArray.SequenceEqual(buffer.ToArray()));
            buffer.Dispose();
        }

        [Test]
        public void ByteBufferMixReadWriteOperation()
        {
            long l = 200;
            int i = 42;
            byte[] array = new byte[] {1, 2, 3, 4};
            
            var buffer = ByteBuffer.Build(0);
            buffer.PutLong(l);
            long l2 = buffer.GetLong(0);

            Assert.AreEqual(l, l2);

            buffer.PutInt(i);
            buffer.Put(array);

            int i2 = buffer.GetInt(sizeof(long));
            Assert.AreEqual(i, i2);

            var bytes2 = buffer.GetBytes(sizeof(long) + sizeof(int), 4);
            Assert.AreEqual(array, bytes2);
        }

        [Test]
        public void ByteBufferWriteBytesArray()
        {
            byte[] array = new byte[] {1, 2, 3, 4};
           
            var buffer = ByteBuffer.Build(0);
            buffer.PutInt(array.Length);
            buffer.Put(array);
            byte[] array2 = buffer.ToArray();
            buffer.Dispose();

            var buffer2 = ByteBuffer.Build(array2);

            int sizeArray = buffer2.GetInt(0);
            byte[] contentArray = buffer2.GetBytes(sizeof(int), sizeArray);
            buffer2.Dispose();

            Assert.AreEqual(4, sizeArray);
            Assert.AreEqual(array, contentArray);
        }
    }
}