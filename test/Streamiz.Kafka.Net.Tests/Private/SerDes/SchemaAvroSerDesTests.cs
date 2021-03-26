using Avro;
using Avro.Specific;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SchemaRegistry.Mock;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Tests.Helpers.Bean.Avro;
using System;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Private.SerDes
{
    public partial class Person : ISpecificRecord
    {
        public static Avro.Schema _SCHEMA = Avro.Schema.Parse("{\"type\":\"record\",\"name\":\"Person\",\"namespace\":\"Streamiz.Kafka.Net.Tests.Private.SerDes\",\"fields\":[{\"name\":\"f" +
                "irstName\",\"type\":\"string\"},{\"name\":\"lastName\",\"type\":\"string\"},{\"name\":\"age\",\"ty" +
                "pe\":\"int\"}]}");
        private string _firstName;
        private string _lastName;
        private int _age;

        public virtual Avro.Schema Schema
        {
            get
            {
                return Person._SCHEMA;
            }
        }

        public string firstName
        {
            get
            {
                return _firstName;
            }
            set
            {
                _firstName = value;
            }
        }
        public string lastName
        {
            get
            {
                return _lastName;
            }
            set
            {
                _lastName = value;
            }
        }
        public int age
        {
            get
            {
                return _age;
            }
            set
            {
                _age = value;
            }
        }
        public virtual object Get(int fieldPos)
        {
            switch (fieldPos)
            {
                case 0: return firstName;
                case 1: return lastName;
                case 2: return age;
                default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
            };
        }
        public virtual void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0: firstName = (System.String)fieldValue; break;
                case 1: lastName = (System.String)fieldValue; break;
                case 2: age = (System.Int32)fieldValue; break;
                default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
            };
        }
    }

    #region Mock

    internal class MockAvroSerDes : SchemaAvroSerDes<Person>
    {
        private readonly MockSchemaRegistryClient mockClient;

        internal MockSchemaRegistryClient Client => mockClient;
        internal AvroSerializer<Person> AvroSerializer => avroSerializer;

        public MockAvroSerDes(MockSchemaRegistryClient mockClient)
        {
            this.mockClient = mockClient;
        }

        protected override ISchemaRegistryClient GetSchemaRegistryClient(SchemaRegistryConfig config)
        {
            mockClient.UseConfiguration(config);
            return mockClient;
        }
    }

    #endregion

    public class SchemaAvroSerDesTests
    {
        internal class MockConfig : IStreamConfig
        {
            public Func<Exception, ExceptionHandlerResponse> InnerExceptionHandler { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public Func<ProcessorContext, ConsumeResult<byte[], byte[]>, Exception, ExceptionHandlerResponse> DeserializationExceptionHandler { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public Func<DeliveryReport<byte[], byte[]>, ExceptionHandlerResponse> ProductionExceptionHandler { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public int? MaxPollIntervalMs { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public long MaxPollRecords { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public long PollMs { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public long CommitIntervalMs { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public int MetadataRequestTimeoutMs { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public TimeSpan TransactionTimeout { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public string TransactionalId { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public string ApplicationId { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public string ClientId { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public int NumStreamThreads { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public ISerDes DefaultKeySerDes { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public ISerDes DefaultValueSerDes { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public ITimestampExtractor DefaultTimestampExtractor { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public ProcessingGuarantee Guarantee { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public string BootstrapServers { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public long MaxTaskIdleMs { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public long BufferedRecordsPerPartition { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }
            public bool FollowMetadata { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

            public void AddAdminConfig(string key, string value)
            {
                throw new NotImplementedException();
            }

            public void AddConfig(string key, string value)
            {
                throw new NotImplementedException();
            }

            public void AddConsumerConfig(string key, string value)
            {
                throw new NotImplementedException();
            }

            public void AddProducerConfig(string key, string value)
            {
                throw new NotImplementedException();
            }

            public IStreamConfig Clone()
            {
                throw new NotImplementedException();
            }

            public AdminClientConfig ToAdminConfig(string clientId)
            {
                throw new NotImplementedException();
            }

            public ConsumerConfig ToConsumerConfig()
            {
                throw new NotImplementedException();
            }

            public ConsumerConfig ToConsumerConfig(string clientId)
            {
                throw new NotImplementedException();
            }

            public ConsumerConfig ToGlobalConsumerConfig(string clientId)
            {
                throw new NotImplementedException();
            }

            public ProducerConfig ToProducerConfig()
            {
                throw new NotImplementedException();
            }

            public ProducerConfig ToProducerConfig(string clientId)
            {
                throw new NotImplementedException();
            }
        }

        readonly string topic = "person";

        [Test]
        public void DeserializeWithoutInit()
        {
            var serdes = new SchemaAvroSerDes<Person>();
            Assert.Throws<StreamsException>(() => serdes.Deserialize(null, new Confluent.Kafka.SerializationContext()));
            Assert.Throws<StreamsException>(() => serdes.DeserializeObject(null, new Confluent.Kafka.SerializationContext()));
        }

        [Test]
        public void SerializeWithoutInit()
        {
            var serdes = new SchemaAvroSerDes<Person>();
            Assert.Throws<StreamsException>(() => serdes.Serialize(null, new Confluent.Kafka.SerializationContext()));
            Assert.Throws<StreamsException>(() => serdes.SerializeObject(null, new Confluent.Kafka.SerializationContext()));
        }

        [Test]
        public void SerializeOK()
        {
            var mockSchemaClient = new MockSchemaRegistryClient();
            var config = new StreamConfig();
            var serdes = new MockAvroSerDes(mockSchemaClient);
            serdes.Initialize(new Net.SerDes.SerDesContext(config));
            var person = new Person { age = 18, firstName = "TEST", lastName = "TEST" };
            var bytes = serdes.Serialize(person, new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic));
            Assert.IsNotNull(bytes);
            Assert.IsTrue(bytes.Length > 0);
        }

        [Test]
        public void DeserializeOK()
        {
            var mockSchemaClient = new MockSchemaRegistryClient();
            var config = new StreamConfig();
            var serdes = new MockAvroSerDes(mockSchemaClient);
            serdes.Initialize(new Net.SerDes.SerDesContext(config));
            var person = new Person { age = 18, firstName = "TEST", lastName = "TEST" };
            var bytes = serdes.Serialize(person, new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic));
            var pbis = serdes.Deserialize(bytes, new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic));
            Assert.AreEqual(18, pbis.age);
            Assert.AreEqual("TEST", pbis.firstName);
            Assert.AreEqual("TEST", pbis.lastName);
        }

        [Test]
        public void CompleteWorkflow()
        {
            var client = new MockSchemaRegistryClient();
            var config = new StreamConfig();
            config.ApplicationId = "test-workflow-avroserdes";
            config.DefaultKeySerDes = new StringSerDes();
            config.DefaultValueSerDes = new MockAvroSerDes(client);

            var builder = new StreamBuilder();
            builder
                .Stream<string, Person>("person")
                .Filter((k, v) => v.age >= 18)
                .To("person-major");

            var topo = builder.Build();
            using (var driver = new TopologyTestDriver(topo, config))
            {
                var input = driver.CreateInputTopic<string, Person>("person");
                var output = driver.CreateOuputTopic<string, Person>("person-major");
                input.PipeInput("test1", new Person { age = 23, firstName = "f", lastName = "l" });
                input.PipeInput("test2", new Person { age = 12, firstName = "f", lastName = "l" });
                var records = output.ReadKeyValueList().ToList();
                Assert.AreEqual(1, records.Count);
                Assert.AreEqual("test1", records[0].Message.Key);
                Assert.AreEqual(23, records[0].Message.Value.age);
                Assert.AreEqual("f", records[0].Message.Value.firstName);
                Assert.AreEqual("l", records[0].Message.Value.lastName);
            }
        }

        [Test]
        public void WorkflowWithInvalidConfiguration()
        {
            var config = new StreamConfig();
            config.ApplicationId = "test-workflow-avroserdes";
            config.DefaultKeySerDes = new StringSerDes();
            config.DefaultValueSerDes = new SchemaAvroSerDes<Person>();

            var builder = new StreamBuilder();
            builder
                .Stream<string, Person>("person")
                .Filter((k, v) => v.age >= 18)
                .To("person-major");

            var topo = builder.Build();
            Assert.Throws<System.ArgumentException>(() =>
            {
                using (var driver = new TopologyTestDriver(topo, config))
                {
                    var input = driver.CreateInputTopic<string, Person>("person");
                    input.PipeInput("test1", new Person { age = 23, firstName = "f", lastName = "l" });
                }
            });
        }

        [Test]
        public void DefautlValue()
        {
            var mockSchemaClient = new MockSchemaRegistryClient();
            var config = new StreamConfig();
            var serdes = new MockAvroSerDes(mockSchemaClient);
            config.ApplicationId = "test-workflow-avroserdes";
            config.DefaultKeySerDes = new StringSerDes();
            config.DefaultValueSerDes = serdes;
            config.SchemaRegistryMaxCachedSchemas = null;
            config.SchemaRegistryRequestTimeoutMs = null;

            var builder = new StreamBuilder();
            builder
                .Stream<string, Person>("person")
                .Filter((k, v) => v.age >= 18)
                .MapValues((v) => v.age)
                .To<StringSerDes, Int32SerDes>("person-major");

            var topo = builder.Build();
            using (var driver = new TopologyTestDriver(topo, config))
            {
                var input = driver.CreateInputTopic<string, Person>("person");
                var output = driver.CreateOuputTopic<string, int, StringSerDes, Int32SerDes>("person-major");
                input.PipeInput("test1", new Person { age = 23, firstName = "f", lastName = "l" });
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
            config.AddConsumerConfig("allow.auto.create.topics", "false");
            config.MaxTaskIdleMs = 50;

            StreamBuilder builder = new StreamBuilder();

            var ss = builder.Stream<string, Order, StringSerDes, SchemaAvroSerDes<Order>>("test-topic")
            .Peek((k, v) =>
            {
                Console.WriteLine($"Order #  {v.order_id }");
            });

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, Order, StringSerDes, SchemaAvroSerDes<Order>>("test-topic");
                inputTopic.PipeInput("test",
                    new Order
                    {
                        order_id = 12,
                        price = 150,
                        product_id = 1
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
            var config = new StreamConfig<StringSerDes, SchemaAvroSerDes<Order>>();
            config.ApplicationId = "test-mock-registry";
            config.SchemaRegistryUrl = "mock://test";

            StreamBuilder builder = new StreamBuilder();

            builder.Stream<string, Order>("test")
                    .Filter((k, v) => k.Contains("test"))
                    .To("test-output");

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, Order>("test");
                var outputTopic = driver.CreateOuputTopic<string, Order>("test-output", TimeSpan.FromSeconds(5));
                inputTopic.PipeInput("test",
                    new Order
                    {
                        order_id = 12,
                        price = 150,
                        product_id = 1
                    });
                var r = outputTopic.ReadKeyValue();
                Assert.IsNotNull(r);
                Assert.AreEqual("test", r.Message.Key);
                Assert.AreEqual(12, r.Message.Value.order_id);
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

            var ss = builder.Stream<string, Order, StringSerDes, SchemaAvroSerDes<Order>>("test-topic")
            .Peek((k, v) =>
            {
                Console.WriteLine($"Order #  {v.order_id }");
            });

            Topology t = builder.Build();

            Assert.Throws<ArgumentException>(() =>
            {
                using (var driver = new TopologyTestDriver(t, config))
                {
                    var inputTopic = driver.CreateInputTopic<string, Order, StringSerDes, SchemaAvroSerDes<Order>>("test-topic");
                    inputTopic.PipeInput("test",
                        new Order
                        {
                            order_id = 12,
                            price = 150,
                            product_id = 1
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

            var ss = builder.Stream<string, Order, StringSerDes, SchemaAvroSerDes<Order>>("test-topic")
            .Peek((k, v) =>
            {
                Console.WriteLine($"Order #  {v.order_id }");
            });

            Topology t = builder.Build();

            Assert.Throws<ArgumentException>(() =>
            {
                using (var driver = new TopologyTestDriver(t, config))
                {
                    var inputTopic = driver.CreateInputTopic<string, Order, StringSerDes, SchemaAvroSerDes<Order>>("test-topic");
                    inputTopic.PipeInput("test",
                        new Order
                        {
                            order_id = 12,
                            price = 150,
                            product_id = 1
                        });
                }
            });
        }
    
        [Test]
        public void IncorrectConfigurationInterface()
        {
            var mockSchemaClient = new MockSchemaRegistryClient();
            var config = new MockConfig();
            var serdes = new MockAvroSerDes(mockSchemaClient);
            Assert.Throws<StreamConfigException>(() => serdes.Initialize(new Net.SerDes.SerDesContext(config)));
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

            var serdes = new MockAvroSerDes(mockSchemaClient);
            serdes.Initialize(new Net.SerDes.SerDesContext(config));

            Assert.AreEqual(1, mockSchemaClient.MaxCachedSchemas);
            Assert.AreEqual(30, mockSchemaClient.RequestTimeoutMs);
        }
    }
}