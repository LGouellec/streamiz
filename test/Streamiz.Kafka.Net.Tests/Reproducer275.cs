using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
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
    public class CloudEventSerDes2 : AbstractSerDes<CloudEvent>
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
    
    public class Reproducer275
    {
        
        [Test]
        public void HeadersAreAssignedToCorrectEvent()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";

            var topology = BuildTopology();

            using (var driver = new TopologyTestDriver(topology, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string, StringSerDes, StringSerDes>("Input");
                var outputTopic = driver.CreateOuputTopic<string, CloudEvent, StringSerDes, CloudEventSerDes2>("Output", TimeSpan.FromSeconds(5));

                IList<CloudEvent> result;

                inputTopic.PipeInput("numbers", "one,two");
                result = outputTopic.ReadValueList().ToList();
                Assert.AreEqual(2, result.Count);
                Assert.AreEqual("one", result[0].Type); // <-- This line fails - it contains the value "two".
                Assert.AreEqual("two", result[1].Type); // <-- This line passes
            }
        }

        private Topology BuildTopology()
        {
            var builder = new StreamBuilder();

            var inputStream = builder.Stream<string, string>("Input", new StringSerDes(), new StringSerDes());

            inputStream
                .FlatMapValues((k, v) => v.Split(",").Select(x => new CloudEvent() { Id = Guid.NewGuid().ToString(), Source = new Uri("abc://a/b/c"), Type = x }))
                .To("Output", new StringSerDes(), new CloudEventSerDes2());

            return builder.Build();
        }
    }
}