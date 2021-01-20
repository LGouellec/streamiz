using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using System;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class StreamizMetadataTests
    {
        /** HEADERS **/
        [Test]
        public void GetCurrentHeadersMetadataTests()
        {
            var source = new System.Threading.CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 1;
            config.FollowMetadata = true;
            var configConsumer = config.Clone();
            configConsumer.ApplicationId = "test-consumer";
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
                })
                .To("output");

            var topo = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(configConsumer.ToConsumerConfig(), null);

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

            consumer.Subscribe("output");
            ConsumeResult<byte[], byte[]> result = null;
            do
            {
                result = consumer.Consume(100);
            } while (result == null);


            source.Cancel();
            thread.Dispose();

            Assert.NotNull(h);
            Assert.AreEqual(1, h.Count);
            Assert.AreEqual("k", h[0].Key);
            Assert.AreEqual(new byte[1] { 13 }, h[0].GetValueBytes());
        }

        [Test]
        public void GetCurrentHeadersMetadataTestsNotConfigured()
        {
            var source = new System.Threading.CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 1;
            var configConsumer = config.Clone();
            configConsumer.ApplicationId = "test-consumer";
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
                })
                .To("output");

            var topo = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(configConsumer.ToConsumerConfig(), null);

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

            consumer.Subscribe("output");
            ConsumeResult<byte[], byte[]> result = null;
            do
            {
                result = consumer.Consume(100);
            } while (result == null);


            source.Cancel();
            thread.Dispose();

            Assert.Null(h);
        }
        /** HEADERS **/

        /** TOPIC **/
        [Test]
        public void GetCurrentTopicMetadataTests()
        {
            var source = new System.Threading.CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 1;
            config.FollowMetadata = true;
            var configConsumer = config.Clone();
            configConsumer.ApplicationId = "test-consumer";
            String h = null;

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .MapValues((v) =>
                {
                    h = StreamizMetadata.GetCurrentTopicMetadata();
                    return v;
                })
                .To("output");

            var topo = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(configConsumer.ToConsumerConfig(), null);

            var thread = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;

            thread.Start(source.Token);
            producer.Produce("topic", new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key1", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });

            consumer.Subscribe("output");
            ConsumeResult<byte[], byte[]> result = null;
            do
            {
                result = consumer.Consume(100);
            } while (result == null);


            source.Cancel();
            thread.Dispose();

            Assert.NotNull(h);
            Assert.AreEqual("topic", h);
        }

        [Test]
        public void GetCurrentTopicMetadataTestsNotConfigured()
        {
            var source = new System.Threading.CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 1;
            var configConsumer = config.Clone();
            configConsumer.ApplicationId = "test-consumer";
            String h = null;

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .MapValues((v) =>
                {
                    h = StreamizMetadata.GetCurrentTopicMetadata();
                    return v;
                })
                .To("output");

            var topo = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(configConsumer.ToConsumerConfig(), null);

            var thread = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;

            thread.Start(source.Token);
            producer.Produce("topic", new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key1", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });

            consumer.Subscribe("output");
            ConsumeResult<byte[], byte[]> result = null;
            do
            {
                result = consumer.Consume(100);
            } while (result == null);


            source.Cancel();
            thread.Dispose();

            Assert.Null(h);
        }
        /** TOPIC **/

        /** OFFSET **/
        [Test]
        public void GetCurrentOffsetMetadataTests()
        {
            var source = new System.Threading.CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 1;
            config.FollowMetadata = true;
            var configConsumer = config.Clone();
            configConsumer.ApplicationId = "test-consumer";
            long? h = null;

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .MapValues((v) =>
                {
                    h = StreamizMetadata.GetCurrentOffsetMetadata();
                    return v;
                })
                .To("output");

            var topo = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(configConsumer.ToConsumerConfig(), null);

            var thread = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;

            thread.Start(source.Token);
            producer.Produce("topic", new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key1", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });

            consumer.Subscribe("output");
            ConsumeResult<byte[], byte[]> result = null;
            do
            {
                result = consumer.Consume(100);
            } while (result == null);


            source.Cancel();
            thread.Dispose();

            Assert.NotNull(h);
            Assert.AreEqual(0, h);
        }

        [Test]
        public void GetCurrentOffsetMetadataTestsNotConfigured()
        {
            var source = new System.Threading.CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 1;
            var configConsumer = config.Clone();
            configConsumer.ApplicationId = "test-consumer";
            long? h = null;

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .MapValues((v) =>
                {
                    h = StreamizMetadata.GetCurrentOffsetMetadata();
                    return v;
                })
                .To("output");

            var topo = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(configConsumer.ToConsumerConfig(), null);

            var thread = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;

            thread.Start(source.Token);
            producer.Produce("topic", new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key1", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });

            consumer.Subscribe("output");
            ConsumeResult<byte[], byte[]> result = null;
            do
            {
                result = consumer.Consume(100);
            } while (result == null);


            source.Cancel();
            thread.Dispose();

            Assert.Null(h);
        }
        /** OFFSET **/

        /** PARTITION **/
        [Test]
        public void GetCurrentPartitionMetadataTests()
        {
            var source = new System.Threading.CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 1;
            config.FollowMetadata = true;
            var configConsumer = config.Clone();
            configConsumer.ApplicationId = "test-consumer";
            int? h = null;

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .MapValues((v) =>
                {
                    h = StreamizMetadata.GetCurrentPartitionMetadata();
                    return v;
                })
                .To("output");

            var topo = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(configConsumer.ToConsumerConfig(), null);

            var thread = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;

            thread.Start(source.Token);
            producer.Produce("topic", new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key1", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });

            consumer.Subscribe("output");
            ConsumeResult<byte[], byte[]> result = null;
            do
            {
                result = consumer.Consume(100);
            } while (result == null);


            source.Cancel();
            thread.Dispose();

            Assert.NotNull(h);
            Assert.AreEqual(0, h);
        }

        [Test]
        public void GetCurrentPartitionMetadataTestsNotConfigured()
        {
            var source = new System.Threading.CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 1;
            var configConsumer = config.Clone();
            configConsumer.ApplicationId = "test-consumer";
            int? h = null;

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .MapValues((v) =>
                {
                    h = StreamizMetadata.GetCurrentPartitionMetadata();
                    return v;
                })
                .To("output");

            var topo = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(configConsumer.ToConsumerConfig(), null);

            var thread = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;

            thread.Start(source.Token);
            producer.Produce("topic", new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key1", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });

            consumer.Subscribe("output");
            ConsumeResult<byte[], byte[]> result = null;
            do
            {
                result = consumer.Consume(100);
            } while (result == null);


            source.Cancel();
            thread.Dispose();

            Assert.Null(h);
        }
        /** PARTITION **/

        /** TIMESTAMP **/
        [Test]
        public void GetCurrentTimestampMetadataTests()
        {
            var source = new System.Threading.CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 1;
            config.FollowMetadata = true;
            var configConsumer = config.Clone();
            configConsumer.ApplicationId = "test-consumer";
            long? h = null;
            DateTime dt = DateTime.Now;

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .MapValues((v) =>
                {
                    h = StreamizMetadata.GetCurrentTimestampMetadata();
                    return v;
                })
                .To("output");

            var topo = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(configConsumer.ToConsumerConfig(), null);

            var thread = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;

            thread.Start(source.Token);
            producer.Produce("topic", new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key1", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });

            consumer.Subscribe("output");
            ConsumeResult<byte[], byte[]> result = null;
            do
            {
                result = consumer.Consume(100);
            } while (result == null);


            source.Cancel();
            thread.Dispose();

            Assert.NotNull(h);
            Assert.IsTrue(h.Value.FromMilliseconds() > dt.ToUniversalTime());
        }

        [Test]
        public void GetCurrentTimestampMetadataTestsNotConfigured()
        {
            var source = new System.Threading.CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 1;
            var configConsumer = config.Clone();
            configConsumer.ApplicationId = "test-consumer";
            long? h = null;

            var serdes = new StringSerDes();
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .MapValues((v) =>
                {
                    h = StreamizMetadata.GetCurrentTimestampMetadata();
                    return v;
                })
                .To("output");

            var topo = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(configConsumer.ToConsumerConfig(), null);

            var thread = StreamThread.Create(
                "thread-0", "c0",
                topo.Builder, config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                0) as StreamThread;

            thread.Start(source.Token);
            producer.Produce("topic", new Confluent.Kafka.Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key1", new SerializationContext()),
                Value = serdes.Serialize("coucou", new SerializationContext())
            });

            consumer.Subscribe("output");
            ConsumeResult<byte[], byte[]> result = null;
            do
            {
                result = consumer.Consume(100);
            } while (result == null);


            source.Cancel();
            thread.Dispose();

            Assert.Null(h);
        }
        /** TIMESTAMP **/

        [Test]
        public void GetUnassignedMetadataTests()
        {
            Assert.Null(StreamizMetadata.GetCurrentHeadersMetadata());
            Assert.Null(StreamizMetadata.GetCurrentOffsetMetadata());
            Assert.Null(StreamizMetadata.GetCurrentPartitionMetadata());
            Assert.Null(StreamizMetadata.GetCurrentTimestampMetadata());
            Assert.Null(StreamizMetadata.GetCurrentTopicMetadata());
        }

    }
}