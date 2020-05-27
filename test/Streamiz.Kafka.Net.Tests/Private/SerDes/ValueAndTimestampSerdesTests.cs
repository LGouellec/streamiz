using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using System;

namespace Streamiz.Kafka.Net.Tests.Private.SerDes
{
    public class ValueAndTimestampSerdesTests
    {
        [Test]
        public void InnerSerdesNull()
        {
            Assert.Throws<StreamsException>(() => new ValueAndTimestampSerDes<string>(null));
        }


        [Test]
        public void SerializeNullData()
        {
            var stringSerdes = new StringSerDes();
            var serdes = new ValueAndTimestampSerDes<string>(stringSerdes);
            var r = serdes.Serialize(null);
            Assert.IsNull(r);
        }

        [Test]
        public void SerializeData()
        {
            long millie = DateTime.Now.Millisecond;
            string s = "coucou";

            var stringSerdes = new StringSerDes();
            var serdes = new ValueAndTimestampSerDes<string>(stringSerdes);
            var data = ValueAndTimestamp<string>.Make(s, millie);
            var r = serdes.Serialize(data);
            Assert.IsNotNull(r);
            Assert.Greater(r.Length, 0);
            var r2 = serdes.Deserialize(r);
            Assert.AreEqual(s, r2.Value);
            Assert.AreEqual(millie, r2.Timestamp);
        }

        [Test]
        public void DeserializeNullData()
        {
            var stringSerdes = new StringSerDes();
            var serdes = new ValueAndTimestampSerDes<string>(stringSerdes);
            var r = serdes.Deserialize(null);
            Assert.IsNull(r);
        }

        [Test]
        public void DeserializeData()
        {
            long millie = DateTime.Now.Millisecond;
            string s = "coucou";

            var stringSerdes = new StringSerDes();
            var serdes = new ValueAndTimestampSerDes<string>(stringSerdes);
            var data = ValueAndTimestamp<string>.Make(s, millie);
            var r = serdes.Deserialize(serdes.Serialize(data));
            Assert.IsNotNull(r);
            Assert.AreEqual(s, r.Value);
            Assert.AreEqual(millie, r.Timestamp);
        }
    }
}
