using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Linq;
using System.Text;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamSetHeadersTests
    {
        internal class TestHeadersTransformer : ITransformer<string, string, string, string>
        {
            private ProcessorContext _context;

            public void Close()
            { }

            public void Init(ProcessorContext<string, string> context)
            {
                this._context = context;
            }

            public Record<string, string> Process(Record<string, string> record)
            {
                Headers headers = record.Headers ?? new Headers();
                headers.Add("headers-test", Encoding.UTF8.GetBytes("headers-1234"));
                
                var newRecord = Record<string, string>.Create(
                    record.Key,
                    record.Value,
                    headers
                );
                
                return newRecord;
            }
        }

        private string GetHeaderValue(IHeader header)
        {
            return Encoding.UTF8.GetString(header.GetValueBytes());
        }

        [Test]
        public void ShouldHaveHeaders()
        {
            StreamConfig config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-set-headers-test";

            StreamBuilder builder = new();

            builder.Stream<string, string>("test")
                .Transform(TransformerBuilder.New<string, string, string, string>()
                    .Transformer<TestHeadersTransformer>()
                    .Build()
                )
                .To("test-output");

            Topology t = builder.Build();
            TopologyTestDriver driver = new(t, config);

            var inputTopic = driver.CreateInputTopic<string, string>("test");
            inputTopic.PipeInput("test-with-headers", "test-1234");

            var outputTopic = driver.CreateOuputTopic<string, string>("test-output", TimeSpan.FromSeconds(10));

            ConsumeResult<string, string> r = outputTopic.ReadKeyValue();
            Assert.AreEqual("test-with-headers", r.Message.Key);
            Assert.AreEqual("test-1234", r.Message.Value);

            Headers rHeaders = r.Message.Headers;
            Assert.IsNotNull(rHeaders);
            Assert.IsTrue(rHeaders.Count > 0);
            IHeader header = rHeaders.FirstOrDefault();
            Assert.AreEqual("headers-test", header.Key);
            Assert.AreEqual("headers-1234", GetHeaderValue(header));
        }

        [Test]
        public void ShouldNotHaveHeaders()
        {
            StreamConfig config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-no-headers-test";

            StreamBuilder builder = new();

            builder.Stream<string, string>("test")
                .To("test-output");

            Topology t = builder.Build();
            TopologyTestDriver driver = new(t, config);

            var inputTopic = driver.CreateInputTopic<string, string>("test");
            inputTopic.PipeInput("test-no-headers", "test-5678");

            var outputTopic = driver.CreateOuputTopic<string, string>("test-output", TimeSpan.FromSeconds(10));
            ConsumeResult<string, string> r = outputTopic.ReadKeyValue();

            Assert.AreEqual("test-no-headers", r.Message.Key);
            Assert.AreEqual("test-5678", r.Message.Value);
            Assert.IsTrue(r.Message.Headers.Count <= 0);
        }

        [Test]
        public void ShouldAddToHeaders()
        {
            StreamConfig config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-add-headers-test";

            StreamBuilder builder = new();

            builder.Stream<string, string>("test")
                .Transform(TransformerBuilder.New<string, string, string, string>()
                    .Transformer<TestHeadersTransformer>()
                    .Build()
                )
                .To("test-output");

            Topology t = builder.Build();
            TopologyTestDriver driver = new(t, config);

            var inputTopic = driver.CreateInputTopic<string, string>("test");
            Headers headers = new()
            {
                { "first-header", Encoding.UTF8.GetBytes("first-test-header") }
            };
            inputTopic.PipeInput("test-add-headers", "test-9012", headers);

            var outputTopic = driver.CreateOuputTopic<string, string>("test-output", TimeSpan.FromSeconds(10));
            ConsumeResult<string, string> r = outputTopic.ReadKeyValue();

            Assert.AreEqual("test-add-headers", r.Message.Key);
            Assert.AreEqual("test-9012", r.Message.Value);
            Assert.IsTrue(r.Message.Headers.Count == 2);

            // Check headers
            Headers expectedHeaders = new()
            {
                { "first-header", Encoding.UTF8.GetBytes("first-test-header") },
                { "headers-test", Encoding.UTF8.GetBytes("headers-1234") }
            };
            for (int i = 0; i < r.Message.Headers.Count; i++)
            {
                IHeader header = r.Message.Headers[i];
                IHeader expectedHeader = expectedHeaders[i];
                Assert.IsNotNull(header);
                Assert.AreEqual(expectedHeader.Key, header.Key);
                Assert.AreEqual(GetHeaderValue(expectedHeader), GetHeaderValue(header));
            }
        }

        [Test]
        public void ShouldHaveHeadersDownstream()
        {
            StreamConfig config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-set-headers-test";

            StreamBuilder builder = new();

            builder.Stream<string, string>("test")
                .Transform(TransformerBuilder.New<string, string, string, string>()
                    .Transformer<TestHeadersTransformer>()
                    .Build()
                )
                .MapValues((value, _) => value.ToUpper())
                .To("test-output");

            Topology t = builder.Build();
            TopologyTestDriver driver = new(t, config);

            var inputTopic = driver.CreateInputTopic<string, string>("test");
            inputTopic.PipeInput("test-with-headers", "test-1234");

            var outputTopic = driver.CreateOuputTopic<string, string>("test-output", TimeSpan.FromSeconds(10));

            ConsumeResult<string, string> r = outputTopic.ReadKeyValue();
            Assert.AreEqual("test-with-headers", r.Message.Key);
            Assert.AreEqual("TEST-1234", r.Message.Value);

            Headers rHeaders = r.Message.Headers;
            Assert.IsNotNull(rHeaders);
            Assert.IsTrue(rHeaders.Count > 0);
            IHeader header = rHeaders.FirstOrDefault();
            Assert.AreEqual("headers-test", header.Key);
            Assert.AreEqual("headers-1234", GetHeaderValue(header));
        }
    }
}
