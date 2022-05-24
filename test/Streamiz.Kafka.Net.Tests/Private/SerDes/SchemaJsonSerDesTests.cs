using Newtonsoft.Json;
using Confluent.SchemaRegistry;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Mock;
using Streamiz.Kafka.Net.SerDes;
using System.Linq;
using Confluent.SchemaRegistry.Serdes;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Json;

namespace Streamiz.Kafka.Net.Tests.Private.SerDes
{
    class PersonJson
    {
        [JsonRequired] // use Newtonsoft.Json annotations
        [JsonProperty("firstName")]
        public string FirstName { get; set; }

        [JsonRequired]
        [JsonProperty("lastName")]
        public string LastName { get; set; }

        [System.ComponentModel.DataAnnotations.Range(0, 150)] // or System.ComponentModel.DataAnnotations annotations
        [JsonProperty("age")]
        public int Age { get; set; }
    }
    
    public class SchemaJsonSerDesTests
    {
        readonly string topic = "person";

        #region Mock

        internal class MockJsonSerDes : SchemaJsonSerDes<PersonJson>
        {
            private readonly MockSchemaRegistryClient mockClient;

            internal MockSchemaRegistryClient Client => mockClient;
            internal JsonSerializer<Person> JsonSerializer => serializer as JsonSerializer<Person>;

            public MockJsonSerDes(MockSchemaRegistryClient mockClient)
            {
                this.mockClient = mockClient;
            }

            protected override ISchemaRegistryClient GetSchemaRegistryClient(SchemaRegistryConfig config)
            {
                mockClient.UseConfiguration(config);
                return mockClient;
            }
        }

        #endregion Mock
        
        [Test]
        public void DeserializeWithoutInit()
        {
            var serdes = new SchemaJsonSerDes<PersonJson>();
            Assert.Throws<StreamsException>(() => serdes.Deserialize(null, new Confluent.Kafka.SerializationContext()));
            Assert.Throws<StreamsException>(() =>
                serdes.DeserializeObject(null, new Confluent.Kafka.SerializationContext()));
        }

        [Test]
        public void SerializeWithoutInit()
        {
            var serdes = new SchemaJsonSerDes<PersonJson>();
            Assert.Throws<StreamsException>(() => serdes.Serialize(null, new Confluent.Kafka.SerializationContext()));
            Assert.Throws<StreamsException>(() =>
                serdes.SerializeObject(null, new Confluent.Kafka.SerializationContext()));
        }

        [Test]
        public void SerializeOK()
        {
            var client = new MockSchemaRegistryClient();
            var config = new StreamConfig();
            var serdes = new MockJsonSerDes(client);
            serdes.Initialize(new Net.SerDes.SerDesContext(config));
            var person = new PersonJson {Age = 18, FirstName = "TEST", LastName = "TEST"};
            var bytes = serdes.Serialize(person,
                new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic));
            Assert.IsNotNull(bytes);
            Assert.IsTrue(bytes.Length > 0);
        }

        [Test]
        public void DeserializeOK()
        {
            var client = new MockSchemaRegistryClient();
            var config = new StreamConfig();
            var serdes = new MockJsonSerDes(client);
            serdes.Initialize(new Net.SerDes.SerDesContext(config));
            var person = new PersonJson {Age = 18, FirstName = "TEST", LastName = "TEST"};
            var bytes = serdes.Serialize(person,
                new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic));
            var pbis = serdes.Deserialize(bytes,
                new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic));
            Assert.AreEqual(18, pbis.Age);
            Assert.AreEqual("TEST", pbis.FirstName);
            Assert.AreEqual("TEST", pbis.LastName);
        }

        [Test]
        public void CompleteWorkflow()
        {
            var client = new MockSchemaRegistryClient();
            var config = new StreamConfig();
            config.ApplicationId = "test-workflow-jsonserdes";
            config.DefaultKeySerDes = new StringSerDes();
            config.DefaultValueSerDes = new MockJsonSerDes(client);

            var builder = new StreamBuilder();
            builder
                .Stream<string, PersonJson>("person")
                .Filter((k, v) => v.Age >= 18)
                .To("person-major");

            var topo = builder.Build();
            using (var driver = new TopologyTestDriver(topo, config))
            {
                var input = driver.CreateInputTopic<string, PersonJson>("person");
                var output = driver.CreateOuputTopic<string, PersonJson>("person-major");
                input.PipeInput("test1", new PersonJson {Age = 23, FirstName = "f", LastName = "l"});
                input.PipeInput("test2", new PersonJson {Age = 12, FirstName = "t", LastName = "i"});
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
            config.ApplicationId = "test-workflow-jsonserdes";
            config.DefaultKeySerDes = new StringSerDes();
            config.DefaultValueSerDes = new SchemaJsonSerDes<Person>();

            var builder = new StreamBuilder();
            builder
                .Stream<string, PersonJson>("person")
                .Filter((k, v) => v.Age >= 18)
                .To("person-major");

            var topo = builder.Build();
            Assert.Throws<System.ArgumentException>(() =>
            {
                using (var driver = new TopologyTestDriver(topo, config))
                {
                    var input = driver.CreateInputTopic<string, PersonJson>("person");
                    input.PipeInput("test1", new PersonJson {Age = 23, FirstName = "f", LastName = "l"});
                }
            });
        }

        [Test]
        public void DefautlValue()
        {
            var mockSchemaClient = new MockSchemaRegistryClient();
            var config = new StreamConfig();
            var serdes = new MockJsonSerDes(mockSchemaClient);
            config.ApplicationId = "test-workflow-jsonserdes";
            config.DefaultKeySerDes = new StringSerDes();
            config.DefaultValueSerDes = serdes;
            config.SchemaRegistryMaxCachedSchemas = null;
            config.SchemaRegistryRequestTimeoutMs = null;

            var builder = new StreamBuilder();
            builder
                .Stream<string, PersonJson>("person")
                .Filter((k, v) => v.Age >= 18)
                .MapValues((v) => v.Age)
                .To<StringSerDes, Int32SerDes>("person-major");

            var topo = builder.Build();
            using (var driver = new TopologyTestDriver(topo, config))
            {
                var input = driver.CreateInputTopic<string, PersonJson>("person");
                var output = driver.CreateOuputTopic<string, int, StringSerDes, Int32SerDes>("person-major");
                input.PipeInput("test1", new PersonJson {Age = 23, FirstName = "f", LastName = "l"});
                var record = output.ReadKeyValue();
                Assert.IsNotNull(record);
                Assert.AreEqual("test1", record.Message.Key);
                Assert.AreEqual(23, record.Message.Value);
            }
        }
        
        [Test]
        public void IncorrectConfigurationInterface()
        {
            var mockSchemaClient = new MockSchemaRegistryClient();
            var mock = new Moq.Mock<IStreamConfig>();
            var serdes = new MockJsonSerDes(mockSchemaClient);
            Assert.Throws<StreamConfigException>(() => serdes.Initialize(new Net.SerDes.SerDesContext(mock.Object)));
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

            var serdes = new MockJsonSerDes(mockSchemaClient);
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
            config.BasicAuthCredentialsSource = (int) AuthCredentialsSource.UserInfo;
            config.SchemaRegistryMaxCachedSchemas = 1;
            config.SchemaRegistryRequestTimeoutMs = 30;

            var serdes = new SchemaJsonSerDes<Person>();
            var schemaConfig = serdes.ToConfig(config);

            Assert.AreEqual(1, schemaConfig.MaxCachedSchemas);
            Assert.AreEqual(30, schemaConfig.RequestTimeoutMs);
            Assert.AreEqual("mock://test", schemaConfig.Url);
            Assert.AreEqual("user:password", schemaConfig.BasicAuthUserInfo);
            Assert.AreEqual(AuthCredentialsSource.UserInfo, schemaConfig.BasicAuthCredentialsSource);
        }

        [Test]
        public void SchemaRegistryJsonSerializerConfig()
        {
            var config = new StreamConfig
            {
                SubjectNameStrategy = SubjectNameStrategy.TopicRecord,
                AutoRegisterSchemas = true,
                UseLatestVersion = false,
                BufferBytes = 1024
            };

            var serdes = new SchemaJsonSerDes<PersonJson>();
            var schemaConfig = serdes.ToSerializerConfig(config);

            Assert.AreEqual(Confluent.SchemaRegistry.SubjectNameStrategy.TopicRecord, schemaConfig.SubjectNameStrategy);
            Assert.AreEqual(true, schemaConfig.AutoRegisterSchemas);
            Assert.AreEqual(false, schemaConfig.UseLatestVersion);
            Assert.AreEqual(1024, schemaConfig.BufferBytes);
        }

    }
}