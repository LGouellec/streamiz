﻿using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Mock;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Protobuf;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Tests.Helpers.Proto;
using System;
using System.Linq;
using Google.Protobuf;

namespace Streamiz.Kafka.Net.Tests.Private.SerDes
{
    #region Mock

    internal class MockProtoSerDesBase<T> : SchemaProtobufSerDes<T> where T : class, IMessage<T>, new()
    {
        private readonly MockSchemaRegistryClient mockClient;

        public MockProtoSerDesBase(MockSchemaRegistryClient mockClient)
        {
            this.mockClient = mockClient;
        }

        protected override ISchemaRegistryClient GetSchemaRegistryClient(SchemaRegistryConfig config)
        {
            mockClient.UseConfiguration(config);
            return mockClient;
        }
    }

    internal class MockProtoSerDes : MockProtoSerDesBase<Helpers.Proto.Person>
    {
        public MockProtoSerDes(MockSchemaRegistryClient mockClient) : base(mockClient)
        {
        }
    }

    internal class MockIncludeProtoSerDes : MockProtoSerDesBase<Helpers.Proto.IncludeConflueType>
    {
        public MockIncludeProtoSerDes(MockSchemaRegistryClient mockClient) : base(mockClient)
        {
        }
    }

    #endregion Mock

    public class SchemaProtobufSerDesTests
    {
        private readonly string topic = "person";

        [Test]
        public void DeserializeWithoutInit()
        {
            var serdes = new SchemaProtobufSerDes<Helpers.Proto.Person>();
            Assert.Throws<StreamsException>(() => serdes.Deserialize(null, new Confluent.Kafka.SerializationContext()));
            Assert.Throws<StreamsException>(() =>
                serdes.DeserializeObject(null, new Confluent.Kafka.SerializationContext()));
        }

        [Test]
        public void SerializeWithoutInit()
        {
            var serdes = new SchemaProtobufSerDes<Helpers.Proto.Person>();
            Assert.Throws<StreamsException>(() => serdes.Serialize(null, new Confluent.Kafka.SerializationContext()));
            Assert.Throws<StreamsException>(() =>
                serdes.SerializeObject(null, new Confluent.Kafka.SerializationContext()));
        }


        [Test]
        public void DeserializeWithExternalType()
        {
            var serdes = new SchemaProtobufSerDes<Helpers.Proto.IncludeConflueType>();
            Assert.Throws<StreamsException>(() => serdes.Deserialize(null, new Confluent.Kafka.SerializationContext()));
            Assert.Throws<StreamsException>(() =>
                serdes.DeserializeObject(null, new Confluent.Kafka.SerializationContext()));
        }


        [Test]
        public void SerializeWithExternalType()
        {
            var serdes = new SchemaProtobufSerDes<Helpers.Proto.IncludeConflueType>();
            Assert.Throws<StreamsException>(() => serdes.Serialize(null, new Confluent.Kafka.SerializationContext()));
            Assert.Throws<StreamsException>(() =>
                serdes.SerializeObject(null, new Confluent.Kafka.SerializationContext()));
        }

        [Test]
        public void SerializeOK()
        {
            var mockSchemaClient = new MockSchemaRegistryClient();
            var config = new StreamConfig();
            var serdes = new MockProtoSerDes(mockSchemaClient);
            serdes.Initialize(new Net.SerDes.SerDesContext(config));
            var person = new Helpers.Proto.Person {Age = 18, FirstName = "TEST", LastName = "TEST"};
            var bytes = serdes.Serialize(person,
                new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic));
            Assert.IsNotNull(bytes);
            Assert.IsTrue(bytes.Length > 0);
        }

        [Test]
        public void DeserializeOK()
        {
            var mockSchemaClient = new MockSchemaRegistryClient();
            var config = new StreamConfig();
            var serdes = new MockProtoSerDes(mockSchemaClient);
            serdes.Initialize(new Net.SerDes.SerDesContext(config));
            var person = new Helpers.Proto.Person {Age = 18, FirstName = "TEST", LastName = "TEST"};
            var bytes = serdes.Serialize(person,
                new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic));
            var pbis = serdes.Deserialize(bytes,
                new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic));
            Assert.AreEqual(18, pbis.Age);
            Assert.AreEqual("TEST", pbis.FirstName);
            Assert.AreEqual("TEST", pbis.LastName);
        }


        [Test]
        public void SerializeExternalTypeOK()
        {
            var mockSchemaClient = new MockSchemaRegistryClient();
            var config = new StreamConfig();
            var serdes = new MockIncludeProtoSerDes(mockSchemaClient);
            serdes.Initialize(new Net.SerDes.SerDesContext(config));
            var value = new Helpers.Proto.IncludeConflueType
                { INTVALUE = 1,
                    DECIMALVALUE = new Confluent.Type.Decimal { Precision = 1, Scale = 2, Value = ByteString.CopyFromUtf8("3") } };
            var bytes = serdes.Serialize(value,
                new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic));
            Assert.IsNotNull(bytes);
            Assert.IsTrue(bytes.Length > 0);
        }

        [Test]
        public void DeserializeExternalTypeOK()
        {
            var mockSchemaClient = new MockSchemaRegistryClient();
            var config = new StreamConfig();
            var serdes = new MockIncludeProtoSerDes(mockSchemaClient);
            serdes.Initialize(new Net.SerDes.SerDesContext(config));
            var value = new Helpers.Proto.IncludeConflueType { INTVALUE = 1,
                DECIMALVALUE = new Confluent.Type.Decimal { Precision = 1, Scale = 2, Value = ByteString.CopyFromUtf8("3") }};
            var bytes = serdes.Serialize(value,
                new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic));
            var pbis = serdes.Deserialize(bytes,
                new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic));
            Assert.AreEqual(value.INTVALUE, pbis.INTVALUE);
            Assert.AreEqual(value.DECIMALVALUE.Precision, pbis.DECIMALVALUE.Precision);
            Assert.AreEqual(value.DECIMALVALUE.Scale, pbis.DECIMALVALUE.Scale);
            Assert.AreEqual(value.DECIMALVALUE.Value, pbis.DECIMALVALUE.Value);
        }

        [Test]
        public void CompleteWorkflow()
        {
            var client = new MockSchemaRegistryClient();
            var config = new StreamConfig();
            config.ApplicationId = "test-workflow-avroserdes";
            config.DefaultKeySerDes = new StringSerDes();
            config.DefaultValueSerDes = new MockProtoSerDes(client);

            var builder = new StreamBuilder();
            builder
                .Stream<string, Helpers.Proto.Person>("person")
                .Filter((k, v, _) => v.Age >= 18)
                .To("person-major");

            var topo = builder.Build();
            using (var driver = new TopologyTestDriver(topo, config))
            {
                var input = driver.CreateInputTopic<string, Helpers.Proto.Person>("person");
                var output = driver.CreateOuputTopic<string, Helpers.Proto.Person>("person-major");
                input.PipeInput("test1", new Helpers.Proto.Person {Age = 23, FirstName = "f", LastName = "l"});
                input.PipeInput("test2", new Helpers.Proto.Person {Age = 12, FirstName = "f", LastName = "l"});
                var records = output.ReadKeyValueList().ToList();
                Assert.AreEqual(1, records.Count);
                Assert.AreEqual("test1", records[0].Message.Key);
                Assert.AreEqual(23, records[0].Message.Value.Age);
                Assert.AreEqual("f", records[0].Message.Value.FirstName);
                Assert.AreEqual("l", records[0].Message.Value.LastName);
            }
        }

        [Test]
        public void WorkflowWithInvalidConfiguration()
        {
            var config = new StreamConfig();
            config.ApplicationId = "test-workflow-avroserdes";
            config.DefaultKeySerDes = new StringSerDes();
            config.DefaultValueSerDes = new SchemaProtobufSerDes<Helpers.Proto.Person>();

            var builder = new StreamBuilder();
            builder
                .Stream<string, Helpers.Proto.Person>("person")
                .Filter((k, v, _) => v.Age >= 18)
                .To("person-major");

            var topo = builder.Build();
            Assert.Throws<System.ArgumentException>(() =>
            {
                using (var driver = new TopologyTestDriver(topo, config))
                {
                    var input = driver.CreateInputTopic<string, Helpers.Proto.Person>("person");
                    input.PipeInput("test1", new Helpers.Proto.Person {Age = 23, FirstName = "f", LastName = "l"});
                }
            });
        }

        [Test]
        public void DefautlValue()
        {
            var mockSchemaClient = new MockSchemaRegistryClient();
            var config = new StreamConfig();
            var serdes = new MockProtoSerDes(mockSchemaClient);
            config.ApplicationId = "test-workflow-avroserdes";
            config.DefaultKeySerDes = new StringSerDes();
            config.DefaultValueSerDes = serdes;
            config.SchemaRegistryMaxCachedSchemas = null;
            config.SchemaRegistryRequestTimeoutMs = null;

            var builder = new StreamBuilder();
            builder
                .Stream<string, Helpers.Proto.Person>("person")
                .Filter((k, v, _) => v.Age >= 18)
                .MapValues((v, _) => v.Age)
                .To<StringSerDes, Int32SerDes>("person-major");

            var topo = builder.Build();
            using (var driver = new TopologyTestDriver(topo, config))
            {
                var input = driver.CreateInputTopic<string, Helpers.Proto.Person>("person");
                var output = driver.CreateOuputTopic<string, int, StringSerDes, Int32SerDes>("person-major");
                input.PipeInput("test1", new Helpers.Proto.Person {Age = 23, FirstName = "f", LastName = "l"});
                var record = output.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("test1", record.Message.Key);
                Assert.AreEqual(23, record.Message.Value);
            }
        }

        [Test]
        public void TestMockSchemaRegistry()
        {
            var config = new StreamConfig();
            config.ApplicationId = "app-test";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.NumStreamThreads = 1;
            config.SchemaRegistryUrl = "mock://test";

            config.Acks = Acks.All;
            config.AddConfig("consumer.allow.auto.create.topics", false);
            config.MaxTaskIdleMs = 50;

            StreamBuilder builder = new StreamBuilder();

            var ss = builder.Stream<string, Order, StringSerDes, SchemaProtobufSerDes<Order>>("test-topic")
                .Peek((k, v, _) => { Console.WriteLine($"Order #  {v.OrderId}"); });

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic =
                    driver.CreateInputTopic<string, Order, StringSerDes, SchemaProtobufSerDes<Order>>("test-topic");
                inputTopic.PipeInput("test",
                    new Order
                    {
                        OrderId = 12,
                        Price = 150,
                        ProductId = 1
                    });
            }

            var client = MockSchemaRegistry.GetClientForScope("test");
            Assert.IsAssignableFrom<MockSchemaRegistryClient>(client);
            Assert.NotNull(client.GetSchemaAsync(1).GetAwaiter().GetResult());

            MockSchemaRegistry.DropScope("test");
        }

        [Test]
        public void TestMockSchemaRegistryInputOutput()
        {
            var config = new StreamConfig<StringSerDes, SchemaProtobufSerDes<Order>>();
            config.ApplicationId = "test-mock-registry";
            config.SchemaRegistryUrl = "mock://test";

            StreamBuilder builder = new StreamBuilder();

            builder.Stream<string, Order>("test")
                .Filter((k, v, _) => k.Contains("test"))
                .To("test-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, Order>("test");
                var outputTopic = driver.CreateOuputTopic<string, Order>("test-output", TimeSpan.FromSeconds(5));
                inputTopic.PipeInput("test",
                    new Order
                    {
                        OrderId = 12,
                        Price = 150,
                        ProductId = 1
                    });
                var r = outputTopic.ReadKeyValue();
                Assert.IsNotNull(r);
                Assert.AreEqual("test", r.Message.Key);
                Assert.AreEqual(12, r.Message.Value.OrderId);
            }

            MockSchemaRegistry.DropScope("test");
        }

        [Test]
        public void TestMockSchemaRegistryExceptionConfiguration()
        {
            var config = new StreamConfig();
            config.ApplicationId = "app-test";
            config.SchemaRegistryUrl = "mock://test1,mock://test2";

            StreamBuilder builder = new StreamBuilder();

            var ss = builder.Stream<string, Order, StringSerDes, SchemaProtobufSerDes<Order>>("test-topic")
                .Peek((k, v, _) => { Console.WriteLine($"Order #  {v.OrderId}"); });

            Topology t = builder.Build();

            Assert.Throws<ArgumentException>(() =>
            {
                using (var driver = new TopologyTestDriver(t, config))
                {
                    var inputTopic =
                        driver.CreateInputTopic<string, Order, StringSerDes, SchemaProtobufSerDes<Order>>("test-topic");
                    inputTopic.PipeInput("test",
                        new Order
                        {
                            OrderId = 12,
                            Price = 150,
                            ProductId = 1
                        });
                }
            });
        }

        [Test]
        public void TestMockSchemaRegistryExceptionConfiguration2()
        {
            var config = new StreamConfig();
            config.ApplicationId = "app-test";
            config.SchemaRegistryUrl = "mock://test1,http://localhost:8081";

            StreamBuilder builder = new StreamBuilder();

            var ss = builder.Stream<string, Order, StringSerDes, SchemaProtobufSerDes<Order>>("test-topic")
                .Peek((k, v, _) => { Console.WriteLine($"Order #  {v.OrderId}"); });

            Topology t = builder.Build();

            Assert.Throws<ArgumentException>(() =>
            {
                using (var driver = new TopologyTestDriver(t, config))
                {
                    var inputTopic =
                        driver.CreateInputTopic<string, Order, StringSerDes, SchemaProtobufSerDes<Order>>("test-topic");
                    inputTopic.PipeInput("test",
                        new Order
                        {
                            OrderId = 12,
                            Price = 150,
                            ProductId = 1
                        });
                }
            });
        }

        [Test]
        public void IncorrectConfigurationInterface()
        {
            var mockSchemaClient = new MockSchemaRegistryClient();
            var config = new Mock<IStreamConfig>();
            var serdes = new MockProtoSerDes(mockSchemaClient);
            Assert.Throws<StreamConfigException>(() => serdes.Initialize(new Net.SerDes.SerDesContext(config.Object)));
        }

        [Test]
        public void SchemaRegistryConfig()
        {
            var mockSchemaClient = new MockSchemaRegistryClient();
            var config = new StreamConfig();
            config.AutoRegisterSchemas = true;
            config.SchemaRegistryMaxCachedSchemas = 1;
            config.SchemaRegistryRequestTimeoutMs = 30;
            config.SubjectNameStrategy = SubjectNameStrategy.TopicRecord;

            var serdes = new MockProtoSerDes(mockSchemaClient);
            serdes.Initialize(new Net.SerDes.SerDesContext(config));

            Assert.AreEqual(1, mockSchemaClient.MaxCachedSchemas);
            Assert.AreEqual(30, mockSchemaClient.RequestTimeoutMs);
        }


        [Test]
        public void SchemaRegistryConfigWithBasicAuth()
        {
            var config = new StreamConfig();
            config.SchemaRegistryUrl = "mock://test";
            config.BasicAuthUserInfo = "user:password";
            config.BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo.ToString();
            config.SchemaRegistryMaxCachedSchemas = 1;
            config.SchemaRegistryRequestTimeoutMs = 30;

            var serdes = new SchemaProtobufSerDes<Order>();
            var schemaConfig = serdes.ToConfig(config, config);

            Assert.AreEqual(1, schemaConfig.MaxCachedSchemas);
            Assert.AreEqual(30, schemaConfig.RequestTimeoutMs);
            Assert.AreEqual("mock://test", schemaConfig.Url);
            Assert.AreEqual("user:password", schemaConfig.BasicAuthUserInfo);
            Assert.AreEqual(AuthCredentialsSource.UserInfo, schemaConfig.BasicAuthCredentialsSource);
        }

        [Test]
        public void SchemaRegistrySerializerConfig()
        {
            var config = new StreamConfig();
            config.SubjectNameStrategy = SubjectNameStrategy.TopicRecord;
            config.AutoRegisterSchemas = true;

            var serdes = new SchemaProtobufSerDes<Order>();
            var schemaConfig = serdes.ToSerializerConfig(config, config);

            Assert.AreEqual(Confluent.SchemaRegistry.SubjectNameStrategy.TopicRecord, schemaConfig.SubjectNameStrategy);
            Assert.AreEqual(true, schemaConfig.AutoRegisterSchemas);
        }

        [Test]
        public void DefaultSchemaRegistryConfig()
        {
            var mockSchemaClient = new MockSchemaRegistryClient();
            var config = new StreamConfig();

            var serdes = new MockProtoSerDes(mockSchemaClient);
            serdes.Initialize(new Net.SerDes.SerDesContext(config));

            Assert.AreEqual(100, mockSchemaClient.MaxCachedSchemas);
            Assert.AreEqual(30000, mockSchemaClient.RequestTimeoutMs);
        }
    }
}