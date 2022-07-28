using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.OpenTelemetry;
using Streamiz.Kafka.Net.Metrics.Prometheus;

namespace sample_stream
{
    /// <summary>
    /// Sample program with a passtrought stream, instanciate and dispose with CTRL+ C console event.
    /// </summary>
    internal class Program
    {
        public class Address
        {
            public string city { get; set; }
            public string zip { get; set; }
        }

        public class Person
        {
            public ObjectId _id { get; set; }
            public Address address { get; set; }
            public string name { get; set; }
            public string phone { get; set; }
        }

        
        public static async Task Main(string[] args)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app2";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.StateDir = Path.Combine(".");
            config.CommitIntervalMs = 5000;
            config.Logger = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Information);
                builder.AddLog4Net();
            });
            config.MetricsRecording = MetricsRecordingLevel.DEBUG;
            //config.UsePrometheusReporter(9090, true);
            config.UseOpenTelemetryReporter();

            StreamBuilder builder = new StreamBuilder();

            var client = new MongoClient(
                "mongodb://admin:admin@localhost:27017"
            );
            var database = client.GetDatabase("streamiz");

            builder
                .Stream<string, string>("input")
                .MapValuesAsync(async (record, _) => {
                    var persons = await database
                        .GetCollection<Person>("adress")
                        .FindAsync((p) => p.name.Equals(record.Key))
                        .Result.ToListAsync();
                    return persons.FirstOrDefault()?.address.city;
                },
                    RetryPolicy
                        .NewBuilder()
                        .NumberOfRetry(10)
                        .RetryBackOffMs(100)
                        .RetriableException<Exception>()
                        .RetryBehavior(EndRetryBehavior.BUFFERED)
                        .Build())
                .To("person-city");
            
            // builder
            //     .Stream<string, string>("input")
            //     .ForeachAsync(
            //         async (record, _) =>
            //         {
            //             await database
            //                 .GetCollection<Person>("adress")
            //                 .InsertOneAsync(new Person()
            //                 {
            //                     name = record.Key,
            //                     address = new Address()
            //                     {
            //                         city = record.Value
            //                     }
            //                 });
            //         },
            //         RetryPolicy
            //             .NewBuilder()
            //             .NumberOfRetry(10)
            //             .RetryBackOffMs(100)
            //             .RetriableException<Exception>()
            //             .RetryBehavior(EndRetryBehavior.SKIP)
            //             .Build());
            
            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);
            
            Console.CancelKeyPress += (o, e) => stream.Dispose();

            await stream.StartAsync();
        }
    }
}