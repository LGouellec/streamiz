using Avro;
using Avro.Specific;
using Confluent.SchemaRegistry;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Tests.Helpers;
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

        public MockAvroSerDes(MockSchemaRegistryClient mockClient)
        {
            this.mockClient = mockClient;
        }

        protected override ISchemaRegistryClient GetSchemaRegistryClient(SchemaRegistryConfig config)
            => mockClient;
    }

    #endregion

    public class SchemaAvroSerDesTests
    {
        string topic = "person";

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
            Assert.Throws<StreamsException>(() => serdes.SerializeObject((Person)null, new Confluent.Kafka.SerializationContext()));
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
    
    }
}
