using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class StreamizMetadataTests
    {

        [Test]
        public void GetCurrentHeadersMetadataTests()
        {
            var source = new System.Threading.CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 1;
            Headers h = null;
            Headers headers = new Headers();
            headers.Add("k", new byte[1] { 13 });

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .MapValues((v) =>
                {
                    h = StreamizMetadata.GetCurrentHeadersMetadata();
                    return v;
                });

            var topo = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());

            var thread = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;

            thread.Start(source.Token);
            producer.Produce("topic", new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key1", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext()),
                Headers = headers
            });

            //WAIT STREAMTHREAD PROCESS MESSAGE
            System.Threading.Thread.Sleep(100);

            source.Cancel();
            thread.Dispose();

            Assert.NotNull(h);
            Assert.AreEqual(1, h.Count);
            Assert.AreEqual("k", h[0].Key);
            Assert.AreEqual(new byte[1] { 13 }, h[0].GetValueBytes());
        }
    }
}
