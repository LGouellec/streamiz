using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class RetryPolicyBuilderTests
    {
        [Test]
        public void RetryPolicyExceptionsTests()
        {
            var builder1 = RetryPolicyBuilder.NewBuilder();
            var builder2 = RetryPolicyBuilder.NewBuilder();
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
            var builder1 = RetryPolicyBuilder.NewBuilder();
            var builder2 = RetryPolicyBuilder.NewBuilder();
            builder1.NumberOfRetry(10);
            builder2.NumberOfRetry(2);
            var pol1 = builder1.Build();
            var pol2 = builder2.Build();
            Assert.AreEqual(10, pol1.NumberOfRetry);
            Assert.AreEqual(2, pol2.NumberOfRetry);
        }
    }
}