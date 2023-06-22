using System;
using System.Linq;
using CloudNative.CloudEvents;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.SerDes.CloudEvents;

namespace Streamiz.Kafka.Net.Tests.Private.SerDes
{
    public class CloudEventSerDesTests
    {
        class Order
        {
            public int OrderId { get; set; }
            public string ProductId { get; set; }
            public DateTime OrderTime { get; set; }
        }
        
        [Test]
        public void SerializeData()
        {
            var order = new Order
            {
                OrderId = 120,
                OrderTime = DateTime.Now,
                ProductId = Guid.NewGuid().ToString()
            };
            
            var serdes = new CloudEventSerDes<Order>();
            var r = serdes.Serialize(
                order,
                new SerializationContext(MessageComponentType.Value, "orders", new Headers()));
            
            Assert.IsNotNull(r);
        }

        [Test]
        public void DeserializeData()
        {
            var order = new Order
            {
                OrderId = 120,
                OrderTime = DateTime.Now,
                ProductId = Guid.NewGuid().ToString()
            };

            var context = new SerializationContext(MessageComponentType.Value, "orders", new Headers());
            
            var serdes = new CloudEventSerDes<Order>();
            var r = serdes.Serialize(
                order,
                context
                );

            var newOrder = serdes.Deserialize(
                r, 
                context);
            
            Assert.AreEqual(order.OrderId, newOrder.OrderId);
            Assert.AreEqual(order.OrderTime, newOrder.OrderTime);
            Assert.AreEqual(order.ProductId, newOrder.ProductId);

        }
        
        [Test]
        public void CompleteWorkflow()
        {
            var config = new StreamConfig();
            config.ApplicationId = "test-workflow-cloudevent-serdes";
            config.DefaultKeySerDes = new StringSerDes();
            config.DefaultValueSerDes = new CloudEventSerDes<Order>();

            var builder = new StreamBuilder();
            builder
                .Stream<string, Order>("order")
                .Filter((k, v) => v.OrderId >= 200)
                .To("order-filtered");

            var topo = builder.Build();
            using var driver = new TopologyTestDriver(topo, config);
            var input = driver.CreateInputTopic<string, Order>("order");
            var output = driver.CreateOuputTopic<string, Order>("order-filtered");
            input.PipeInput("order1", new Order() {OrderId = 240, OrderTime = DateTime.Now, ProductId = "123"});
            input.PipeInput("order2", new Order() {OrderId = 40, OrderTime = DateTime.Now, ProductId = "456"});

            var records = output.ReadKeyValueList().ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual("order1", records[0].Message.Key);
            Assert.AreEqual(240, records[0].Message.Value.OrderId);
            Assert.AreEqual("123", records[0].Message.Value.ProductId);
        }
        
        [Test]
        public void CompleteWorkflowWithOverrideConfiguration()
        {
            Func<Order, string> exportOrderId = (o) =>
            {
                return o.OrderId.ToString();
            };
            var config = new StreamConfig();
            config.ApplicationId = "test-workflow-cloudevent-serdes";
            config.DefaultKeySerDes = new StringSerDes();
            config.DefaultValueSerDes = new CloudEventSerDes<Order>();
            
            config.AddConfig(CloudEventSerDesConfig.CloudEventContentMode, ContentMode.Structured);
            config.AddConfig(CloudEventSerDesConfig.CloudEventExportId, exportOrderId);
            
            var builder = new StreamBuilder();
            builder
                .Stream<string, Order>("order")
                .Filter((k, v) => v.OrderId >= 200)
                .To("order-filtered");

            var topo = builder.Build();
            using var driver = new TopologyTestDriver(topo, config);
            var input = driver.CreateInputTopic<string, Order>("order");
            var output = driver.CreateOuputTopic<string, Order>("order-filtered");
            input.PipeInput("order1", new Order() {OrderId = 240, OrderTime = DateTime.Now, ProductId = "123"});
            input.PipeInput("order2", new Order() {OrderId = 40, OrderTime = DateTime.Now, ProductId = "456"});

            var records = output.ReadKeyValueList().ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual("order1", records[0].Message.Key);
            Assert.AreEqual(240, records[0].Message.Value.OrderId);
            Assert.AreEqual("123", records[0].Message.Value.ProductId);
        }

        [Test]
        public void KeyException()
        {
            var context = new SerializationContext(MessageComponentType.Key, "orders", new Headers());
            
            var serdes = new CloudEventSerDes<String>();
            
            Assert.Throws<StreamsException>(() => serdes.Serialize(
                "hello",
                context
            ));
            
            Assert.Throws<StreamsException>(() => serdes.Deserialize(
                new byte[0],
                context
            ));
        }
        
        [Test]
        public void NullPayload()
        {

            var context = new SerializationContext(MessageComponentType.Value, "orders", new Headers());
            
            var serdes = new CloudEventSerDes<Order>();
            var r = serdes.Serialize(
                null,
                context
            );
            
            Assert.IsNull(r);

            var newOrder = serdes.Deserialize(
                r, 
                context);

            Assert.IsNull(newOrder);
        }


        [Test]
        public void DeserializeNotCloudEventMessage()
        {
            var context = new SerializationContext(MessageComponentType.Value, "orders", new Headers());
            
            var serdes = new CloudEventSerDes<Order>();

            Assert.Throws<InvalidOperationException>(() => serdes.Deserialize(
                new byte[1] {1},
                context));
        }
    }
}