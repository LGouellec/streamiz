
using System;
using System.IO;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Reproducer229.Model;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Supplier;
using Streamiz.Kafka.Net.Stream;

namespace Reproducer229 {
    internal class Program {

        private static readonly TimeSpan WindowTimespan = TimeSpan.FromDays(4);
        private static readonly TimeSpan WindowSize = TimeSpan.FromDays(4);
        private static readonly TimeSpan JoinWindowSize = TimeSpan.FromDays(2);


        static string GetEnvironmentVariable(string var, string @default)
        {
            return Environment.GetEnvironmentVariable(var) ?? @default;
        }

        public static async Task Main(string[] args)
        {
            string boostrapserver = GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVER", "localhost:9092");


            IWindowBytesStoreSupplier CreateStoreSupplier(string storeName)
            {
                return Stores.PersistentWindowStore(storeName, WindowTimespan,
                    WindowSize, (long)(2 * WindowTimespan.TotalMilliseconds));
            }
            
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app-reproducer229";
            config.BootstrapServers = boostrapserver;
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.CommitIntervalMs = 3000;
            config.Debug = "consumer,cgrp,topic,fetch";
            config.StateDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            config.Logger = LoggerFactory.Create((b) =>
            {
                b.SetMinimumLevel(LogLevel.Information);
                b.AddConsole();
            });
            config.FollowMetadata = true;
            
            StreamBuilder builder = new StreamBuilder();
            
            var ticket1Stream = builder
                .Stream<string, Ticket, StringSerDes, JsonSerDes<Ticket>>("tickets");
            var lotteryTicketDetailsStream = builder
                .Stream<string, TicketDetails, StringSerDes, JsonSerDes<TicketDetails>>("tickets-details");

            var topic1supplier = CreateStoreSupplier("tickets-store");
            var topic2Supplier = CreateStoreSupplier("tickets-details-store");

            var joinValueMapper = new JoinValueMapper();
            var joinWindowOptions = JoinWindowOptions.Of(JoinWindowSize);
            var joinProps =
                StreamJoinProps.With<string, Ticket, TicketDetails>(topic1supplier,
                    topic2Supplier);

            ticket1Stream.Print(
                Printed<string, Ticket>
                .ToOut()
                .WithKeyValueMapper((k,v) => {
                    var topic = StreamizMetadata.GetCurrentTopicMetadata();
                    var offset = StreamizMetadata.GetCurrentOffsetMetadata();
                    var ts = StreamizMetadata.GetCurrentTimestampMetadata();
                    var part = StreamizMetadata.GetCurrentPartitionMetadata();
                    return $"Key:{k}|Value:{JsonConvert.SerializeObject(v)}:(Metadata:{topic}|{part}|{offset}|{ts})";
                }));

            lotteryTicketDetailsStream.Print(
                Printed<string, TicketDetails>
                .ToOut()
                .WithKeyValueMapper((k,v) => {
                    var topic = StreamizMetadata.GetCurrentTopicMetadata();
                    var offset = StreamizMetadata.GetCurrentOffsetMetadata();
                    var ts = StreamizMetadata.GetCurrentTimestampMetadata();
                    var part = StreamizMetadata.GetCurrentPartitionMetadata();
                    return $"Key:{k}|Value:{JsonConvert.SerializeObject(v)}:(Metadata:{topic}|{part}|{offset}|{ts})";
                }));

           ticket1Stream
               .Join(lotteryTicketDetailsStream, joinValueMapper, joinWindowOptions, joinProps)
               .To("output", new StringSerDes(), new JsonSerDes<Progression>());
        
        
            Topology t = builder.Build();
            KafkaStream stream1 = new KafkaStream(t, config);
            
            Console.CancelKeyPress += (_, _) => stream1.Dispose();
            
            await stream1.StartAsync();
        }
    }       
}