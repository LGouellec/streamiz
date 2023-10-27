using System;
using NUnit.Framework;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class StreamTableJoinPropsTests
    {
        [Test]
        public void TestConstructor()
        {
            var join = new StreamTableJoinProps<string, Int32, float>(
                new StringSerDes(),
                new Int32SerDes(),
                new FloatSerDes());
            
            Assert.IsInstanceOf<StringSerDes>(join.KeySerdes);
            Assert.IsInstanceOf<Int32SerDes>(join.LeftValueSerdes);
            Assert.IsInstanceOf<FloatSerDes>(join.RightValueSerdes);
        }
        
        [Test]
        public void TestConstructorWithNull()
        {
            var join = new StreamTableJoinProps<string, Int32, float>(
                null, null, null);
            
            Assert.IsNull(join.KeySerdes);
            Assert.IsNull(join.LeftValueSerdes);
            Assert.IsNull(join.RightValueSerdes);
        }
    }
}