using NUnit.Framework;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Tests.Private.SerDes
{
    public class ByteArraySerDesTests
    {
        [Test]
        public void SerializeNullData()
        {
            var arraySerdes = new ByteArraySerDes();
            var r = arraySerdes.Serialize(null);
            Assert.IsNull(r);
        }

        [Test]
        public void SerializeData()
        {
            byte[] b = new byte[] { 4, 1, 2, 3 };
            var arraySerdes = new ByteArraySerDes();
            var r = arraySerdes.Serialize(b);
            Assert.IsNotNull(r);
            Assert.AreEqual(b, r);
        }


        [Test]
        public void DeserializeNullData()
        {
            var arraySerdes = new ByteArraySerDes(); 
            var r = arraySerdes.Deserialize(null);
            Assert.IsNull(r);
        }

        [Test]
        public void DeserializeData()
        {
            byte[] b = new byte[] { 4, 1, 2, 3 };
            var arraySerdes = new ByteArraySerDes();
            var r = arraySerdes.Deserialize(arraySerdes.Serialize(b));
            Assert.IsNotNull(r);
            Assert.AreEqual(b, r);
        }

    }
}
