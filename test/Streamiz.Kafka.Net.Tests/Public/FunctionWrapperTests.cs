using NUnit.Framework;
using Streamiz.Kafka.Net.Stream;
using System;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class FunctionWrapperTests
    {
        #region Assert Null Function
        [Test]
        public void FunctionNullValueMapper()
        {
            Assert.Throws<ArgumentNullException>(() => new WrappedValueMapper<int, int>(null));
        }

        [Test]
        public void FunctionNullKeyValueMapper()
        {
            Assert.Throws<ArgumentNullException>(() => new WrappedKeyValueMapper<string, int, int>(null));
        }

        [Test]
        public void FunctionNullValueMapperWithKey()
        {
            Assert.Throws<ArgumentNullException>(() => new WrappedValueMapperWithKey<string, int, int>(null));
        }

        [Test]
        public void FunctionNullInitializerMapper()
        {
            Assert.Throws<ArgumentNullException>(() => new WrappedInitializer<int>(null));
        }

        [Test]
        public void FunctionNullAggregatorMapper()
        {
            Assert.Throws<ArgumentNullException>(() => new WrappedAggregator<string, int, int>(null));
        }

        [Test]
        public void FunctionNullReducer()
        {
            Assert.Throws<ArgumentNullException>(() => new WrappedReducer<int>(null));
        }
        #endregion

        [Test]
        public void FunctionValueMapper()
        {
            Func<int, int> mapper = (i) => i * 2;
            var wrap = new WrappedValueMapper<int, int>(mapper);
            Assert.AreEqual(4, wrap.Apply(2));
        }

        [Test]
        public void FunctionKeyValueMapper()
        {
            Func<string, int, int> mapper = (k, v) => k.Length;
            var wrap = new WrappedKeyValueMapper<string, int, int>(mapper);
            Assert.AreEqual(4, wrap.Apply("test", 12));
        }

        [Test]
        public void FunctionValueMapperWithKey()
        {
            Func<string, int, int> mapper = (k, v) => v * 4;
            var wrap = new WrappedValueMapperWithKey<string, int, int>(mapper);
            Assert.AreEqual(12, wrap.Apply("test", 3));
        }

        [Test]
        public void FunctionInitializerMapper()
        {
            Func<int> f = () => 120;
            var wrap = new WrappedInitializer<int>(f);
            Assert.AreEqual(120, wrap.Apply());
        }

        [Test]
        public void FunctionAggregatorMapper()
        {
            int count = 0;
            Func<string, int, int, int> f = (k, v, agg) => agg + 1;
            var wrap = new WrappedAggregator<string, int, int>(f);
            count = wrap.Apply("test", 1, count);
            Assert.AreEqual(2, wrap.Apply("test", 150, count));
        }

        [Test]
        public void FunctionReducer()
        {
            Func<int, int, int> f = (v1, v2) => v2 > v1 ? v2 : v1;
            var wrap = new WrappedReducer<int>(f);
            Assert.AreEqual(12, wrap.Apply(12, 10));
            Assert.AreEqual(100, wrap.Apply(12, 100));
        }
    }
}