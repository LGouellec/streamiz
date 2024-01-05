using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Kafka;
using CloudNative.CloudEvents.SystemTextJson;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests
{
    internal class CloudEventSerDes2 : AbstractSerDes<CloudEvent>
    {
        private CloudEventFormatter formatter;

        public CloudEventSerDes2()
        {
            var jsonOptions = new JsonSerializerOptions();

            formatter = new JsonEventFormatter(jsonOptions, new JsonDocumentOptions());
        }

        public override byte[] Serialize(CloudEvent data, SerializationContext context)
        {
            if (context.Component == MessageComponentType.Key)
            {
                throw new StreamsException("This serdes is only accessible for the value");
            }

            if (data == null)
            {
                return null;
            }

            var tmpMessage = data.ToKafkaMessage(ContentMode.Binary, formatter);
            UpdateCurrentHeader(tmpMessage, context);
            return tmpMessage.Value;
        }

        private void UpdateCurrentHeader(Message<string?, byte[]> tmpMessage, SerializationContext context)
        {
            foreach (var header in tmpMessage.Headers)
            {
                if (context.Headers.TryGetLastBytes(header.Key, out byte[] lastHeader))
                {
                    context.Headers.Remove(header.Key);
                }

                context.Headers.Add(header.Key, header.GetValueBytes());
            }
        }

        public override CloudEvent Deserialize(byte[] data, SerializationContext context)
        {
            if (context.Component == MessageComponentType.Key)
            {
                throw new StreamsException("This serdes is only accessible for the value");
            }

            if (data == null)
            {
                return null;
            }

            var tmpMessage = new Message<string?, byte[]>
            {
                Headers = context.Headers,
                Key = null, // ignore in the extensions method
                Value = data
            };

            if (!tmpMessage.IsCloudEvent())
            {
                throw new InvalidOperationException("The message is not a CloudEvent record." +
                                                    " Some kafka headers are needed to consider this message as a CloudEvent record." +
                                                    " Please refer to the kafka protocol binding in the cloudevents specs.");
            }

            return tmpMessage.ToCloudEvent(formatter);
        }
    }

    public class Reproducer275Tests
    {
        [Test]
        public void HeadersSyncAreAssignedToCorrectEvent()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";

            var topology = BuildTopology();

            using (var driver = new TopologyTestDriver(topology, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string, StringSerDes, StringSerDes>("Input");
                var outputTopic =
                    driver.CreateOuputTopic<string, CloudEvent, StringSerDes, CloudEventSerDes2>("Output",
                        TimeSpan.FromSeconds(5));

                IList<CloudEvent> result;

                inputTopic.PipeInput("numbers", "one,two,three,four");
                result = outputTopic.ReadValueList().ToList();
                Assert.AreEqual(4, result.Count);
                Assert.AreEqual("one", result[0].Type);
                Assert.AreEqual("two", result[1].Type);
                Assert.AreEqual("three", result[2].Type);
                Assert.AreEqual("four", result[3].Type);
            }
        }

        [Test]
        public void HeadersAsyncAreAssignedToCorrectEvent()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";

            var topology = BuildTopology(true);

            using (var driver = new TopologyTestDriver(topology, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string, StringSerDes, StringSerDes>("Input");
                var outputTopic =
                    driver.CreateOuputTopic<string, CloudEvent, StringSerDes, CloudEventSerDes2>("Output",
                        TimeSpan.FromSeconds(5));

                IList<CloudEvent> result;

                inputTopic.PipeInput("numbers", "one,two,three,four");
                result = outputTopic.ReadValueList().ToList();
                Assert.AreEqual(4, result.Count);
                Assert.AreEqual("one", result[0].Type); // <-- This line passes
                Assert.AreEqual("two", result[1].Type); // <-- This line fails - it contains the value "four".
                Assert.AreEqual("three", result[2].Type); // <-- This line fails - it contains the value "four".
                Assert.AreEqual("four", result[3].Type); // <-- This line passes
            }
        }

        private Topology BuildTopology(bool async = false)
        {
            var builder = new StreamBuilder();

            var inputStream = builder.Stream<string, string>("Input", new StringSerDes(), new StringSerDes());

            if(!async)
                inputStream
                    .FlatMapValues((k, v) => v.Split(",").Select(x => new CloudEvent()
                        {Id = Guid.NewGuid().ToString(), Source = new Uri("abc://a/b/c"), Type = x}))
                    .To("Output", new StringSerDes(), new CloudEventSerDes2());
            else
                inputStream
                    .FlatMapValuesAsync(
                        async (record, _) => await Task.FromResult(record.Value.Split(",").Select(x => new CloudEvent()
                        {Id = Guid.NewGuid().ToString(), Source = new Uri("abc://a/b/c"), Type = x})),
                        null, 
                        new RequestSerDes<string, string>(new StringSerDes(), new StringSerDes()),
                        new ResponseSerDes<string, CloudEvent>(new StringSerDes(), new CloudEventSerDes2()))
                    .To("Output", new StringSerDes(), new CloudEventSerDes2());

            return builder.Build();
        }
    }
}