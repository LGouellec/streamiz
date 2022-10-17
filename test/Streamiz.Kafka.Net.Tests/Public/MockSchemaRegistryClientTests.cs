using Confluent.SchemaRegistry;
using NUnit.Framework;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Mock;
using System;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.Tests.Public
{
    [TestFixture]
    public class MockSchemaRegistryClientTests
    {
        private MockSchemaRegistryClient client = null;

        private readonly string schema = "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"Streamiz.Kafka.Net.Tests.Helpers.Bean.Avro\",\"fields\":[{\"name\":\"or" +
                "der_id\",\"type\":\"int\"},{\"name\":\"price\",\"type\":\"float\"},{\"name\":\"product_id\",\"type" +
                "\":\"int\"}]}";

        [SetUp]
        public void Setup()
        {
            client = new MockSchemaRegistryClient();
        }

        [TearDown]
        public void Done()
        {
            client.Dispose();
        }

        [Test]
        public void ConstructKeySubjectNameTest()
        {
            string s = client.ConstructKeySubjectName("test");
            Assert.AreEqual($"test-key", s);
        }

        [Test]
        public void ConstructValueSubjectNameTest()
        {
            string s = client.ConstructValueSubjectName("test");
            Assert.AreEqual($"test-value", s);
        }

        [Test]
        public async Task LookupSchemaAsyncEmptyTest()
        {
            var result = await client.LookupSchemaAsync("subject", null, false);
            Assert.IsNull(result);
        }

        [Test]
        public async Task LookupSchemaAsyncTest()
        {
            await client.RegisterSchemaAsync("order", "schema1");
            await client.RegisterSchemaAsync("order", "schema2");
            var r = await client.LookupSchemaAsync("order", null, true);
            Assert.AreEqual(r.SchemaString, "schema2");
        }

        [Test]
        public async Task RegisterSchemaAsyncTest()
        {
            int result = await client.RegisterSchemaAsync("order", schema);
            Assert.AreEqual(1, result);
        }

        [Test]
        public async Task RegisterSchemaAsyncVersion2Test()
        {
            int result = await client.RegisterSchemaAsync("order", schema);
            Assert.AreEqual(1, result);
            result = await client.RegisterSchemaAsync("order", schema);
            Assert.AreEqual(2, result);
        }

        [Test]
        public async Task RegisterConfluentSchemaAsyncTest()
        {
            Schema s = new Schema(schema, SchemaType.Avro);
            int result = await client.RegisterSchemaAsync("order", s);
            Assert.AreEqual(1, result);
        }

        [Test]
        public async Task IsCompatibleAsyncTest()
        {
            int result = await client.RegisterSchemaAsync("order", schema);
            Assert.AreEqual(1, result);
            bool b = await client.IsCompatibleAsync("order", schema);
            Assert.IsTrue(b);
        }

        [Test]
        public async Task ConfluentIsCompatibleAsyncTest()
        {
            Schema s = new Schema(schema, SchemaType.Avro);
            int result = await client.RegisterSchemaAsync("order", s);
            Assert.AreEqual(1, result);
            bool b = await client.IsCompatibleAsync("order", s);
            Assert.IsTrue(b);
        }

        [Test]
        public async Task GetSubjectVersionsAsyncTest()
        {
            await client.RegisterSchemaAsync("order", schema);
            await client.RegisterSchemaAsync("order", schema);
            var items = await client.GetSubjectVersionsAsync("order");
            Assert.AreEqual(2, items.Count);
            Assert.AreEqual(1, items[0]);
            Assert.AreEqual(2, items[1]);
        }

        [Test]
        public async Task GetSchemaIdAsyncTest()
        {
            await client.RegisterSchemaAsync("order", schema);
            var id = await client.GetSchemaIdAsync("order", schema);
            Assert.AreEqual(1, id);
        }

        [Test]
        public async Task GetUnknownSchemaIdAsyncTest()
        {
            await client.RegisterSchemaAsync("order", schema);
            var id = await client.GetSchemaIdAsync("order", "toto");
            Assert.AreEqual(-1, id);
        }

        [Test]
        public async Task GetUnknownSchemaIdAsyncTest2()
        {
            await client.RegisterSchemaAsync("order", schema);
            var id = await client.GetSchemaIdAsync("product", "toto");
            Assert.AreEqual(-1, id);
        }

        [Test]
        public async Task ConfluentGetSchemaIdAsyncTest()
        {
            Schema s = new Schema(schema, SchemaType.Avro);
            await client.RegisterSchemaAsync("order", s);
            var id = await client.GetSchemaIdAsync("order", s);
            Assert.AreEqual(1, id);
        }

        [Test]
        public async Task GetSchemaAsyncTest()
        {
            await client.RegisterSchemaAsync("order", schema);
            var s = await client.GetSchemaAsync("order", 1);
            Assert.AreEqual(schema, s);
        }

        [Test]
        public async Task GetNullSchemaAsyncTest()
        {
            await client.RegisterSchemaAsync("order", schema);
            var s = await client.GetSchemaAsync("product", 1);
            Assert.IsNull(s);
        }

        [Test]
        public async Task GetSchemaNullAsyncTest()
        {
            await client.RegisterSchemaAsync("order", schema);
            var s = await client.GetSchemaAsync("order", 2);
            Assert.IsNull(s);
        }

        [Test]
        public async Task GetSchemaAsyncTest2()
        {
            await client.RegisterSchemaAsync("order", schema);
            var s = await client.GetSchemaAsync(1);
            Assert.AreEqual(schema, s.SchemaString);
        }

        [Test]
        public async Task GetSchemaNullAsyncTest2()
        {
            await client.RegisterSchemaAsync("order", schema);
            var s = await client.GetSchemaAsync(100);
            Assert.IsNull(s);
        }

        [Test]
        public async Task GetAllSubjectsAsyncTest()
        {
            await client.RegisterSchemaAsync("order", schema);
            await client.RegisterSchemaAsync("product", schema);
            var results = await client.GetAllSubjectsAsync();
            Assert.AreEqual(2, results.Count);
        }

        [Test]
        public async Task GetLatestSchemaAsyncTest()
        {
            await client.RegisterSchemaAsync("order", "schema1");
            await client.RegisterSchemaAsync("order", "schema2");
            var r = await client.GetLatestSchemaAsync("order");
            Assert.AreEqual(r.SchemaString, "schema2");
        }

        [Test]
        public async Task GetLatestSchemaNullAsyncTest()
        {
            await client.RegisterSchemaAsync("order", "schema1");
            await client.RegisterSchemaAsync("order", "schema2");
            var r = await client.GetLatestSchemaAsync("product");
            Assert.IsNull(r);
        }

        [Test]
        public async Task GetRegisteredSchemaAsyncTest()
        {
            await client.RegisterSchemaAsync("order", schema);
            var r = await client.GetRegisteredSchemaAsync("order", 1);
            Assert.AreEqual(r.SchemaString, schema);
            Assert.AreEqual(r.Version, 1);
        }

        [Test]
        public async Task GetNullRegisteredSchemaAsyncTest()
        {
            await client.RegisterSchemaAsync("order", schema);
            var r = await client.GetRegisteredSchemaAsync("order", 2);
            Assert.IsNull(r);
        }

        [Test]
        public async Task GetRegisteredSchemaNullAsyncTest()
        {
            await client.RegisterSchemaAsync("order", schema);
            var r = await client.GetRegisteredSchemaAsync("product", 2);
            Assert.IsNull(r);
        }
    }
}