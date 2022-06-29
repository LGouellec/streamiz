using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class RetryPolicyTests
    {
        [Test]
        public void RetryPolicyExceptionsTests()
        {
            var builder1 = RetryPolicy.NewBuilder();
            var builder2 = RetryPolicy.NewBuilder();
            builder1.RetriableException<StreamsException>();
            builder2.RetriableException<NoneRetryableException>();
            var pol1 = builder1.Build();
            var pol2 = builder2.Build();
            Assert.AreEqual(1, pol1.RetriableExceptions.Count);
            Assert.AreEqual(1, pol2.RetriableExceptions.Count);
        }
        
        [Test]
        public void RetryPolicyNumberRetryTests()
        {
            var builder1 = RetryPolicy.NewBuilder();
            var builder2 = RetryPolicy.NewBuilder();
            builder1.NumberOfRetry(10);
            builder2.NumberOfRetry(2);
            var pol1 = builder1.Build();
            var pol2 = builder2.Build();
            Assert.AreEqual(10, pol1.NumberOfRetry);
            Assert.AreEqual(2, pol2.NumberOfRetry);
        }
        
        [Test]
        public void RetryPolicyBackOffTests()
        {
            var builder1 = RetryPolicy.NewBuilder();
            var builder2 = RetryPolicy.NewBuilder();
            builder1.RetryBackOffMs(120);
            builder2.RetryBackOffMs(50);
            var pol1 = builder1.Build();
            var pol2 = builder2.Build();
            Assert.AreEqual(120, pol1.RetryBackOffMs);
            Assert.AreEqual(50, pol2.RetryBackOffMs);
        }
        
        [Test]
        public void RetryPolicyBufferSize()
        {
            var builder1 = RetryPolicy.NewBuilder();
            var builder2 = RetryPolicy.NewBuilder();
            builder1.MemoryBufferSize(40);
            builder2.MemoryBufferSize(10);
            var pol1 = builder1.Build();
            var pol2 = builder2.Build();
            Assert.AreEqual(40, pol1.MemoryBufferSize);
            Assert.AreEqual(10, pol2.MemoryBufferSize);
        }
        
        [Test]
        public void RetryPolicyTimeout()
        {
            var builder1 = RetryPolicy.NewBuilder();
            var builder2 = RetryPolicy.NewBuilder();
            builder1.TimeoutMs(300);
            builder2.TimeoutMs(600);
            var pol1 = builder1.Build();
            var pol2 = builder2.Build();
            Assert.AreEqual(300, pol1.TimeoutMs);
            Assert.AreEqual(600, pol2.TimeoutMs);
        }
        
        [Test]
        public void RetryPolicyBehavior()
        {
            var builder1 = RetryPolicy.NewBuilder();
            var builder2 = RetryPolicy.NewBuilder();
            builder1.RetryBehavior(EndRetryBehavior.FAIL);
            builder2.RetryBehavior(EndRetryBehavior.BUFFERED);
            var pol1 = builder1.Build();
            var pol2 = builder2.Build();
            Assert.AreEqual(EndRetryBehavior.FAIL, pol1.EndRetryBehavior);
            Assert.AreEqual(EndRetryBehavior.BUFFERED, pol2.EndRetryBehavior);
        }
    }
}