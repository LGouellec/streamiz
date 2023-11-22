using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Linq;
using System.Text.Json;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Kafka;
using CloudNative.CloudEvents.SystemTextJson;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace sample_stream
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
    
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = $"test-app",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
            
            var t = BuildTopology();
            var stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) =>
            {
                stream.Dispose();
            };
            
            await stream.StartAsync();
        }
        
        private static Topology BuildTopology()
        {
            var builder = new StreamBuilder();
            
            var globalTable = builder
                .GlobalTable("Input2", new Int64SerDes(), new StringSerDes());
            
            var inputStream = builder
                .Stream<string, string>("Input", new StringSerDes(), new StringSerDes())
                .Map((k, v) => KeyValuePair.Create(1L, v.Length))
                .LeftJoin<long, string, int>(globalTable, (k1, k2) => k1, (sO, g) =>
                {
                    return sO;
                })
                .GroupByKey<Int64SerDes, Int32SerDes>()
                .Aggregate(() => new List<int>(), (l, i, arg3) =>
                {
                    arg3.Add(i);
                    return arg3;
                });

            return builder.Build();
        }
    }
}