using System;
using System.Diagnostics.Metrics;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry.Encryption;
using Confluent.SchemaRegistry.Encryption.Aws;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RocksDbSharp;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Json;
using OpenTelemetry;
using Streamiz.Kafka.Net.Metrics.OpenTelemetry;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using SubjectNameStrategy = Confluent.SchemaRegistry.SubjectNameStrategy;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Prometheus;

namespace sample_stream
{
    public static class Program
    {
        public class Address {

            [JsonProperty]
            private int doornumber;

            [JsonProperty]
            private String doorpin;

            [JsonProperty]
            private String state;

            [JsonProperty]
            private String street;
            
            public Address(){}

            public Address(int doornumber, String doorpin, String state, String street) {
                this.doornumber = doornumber;
                this.doorpin = doorpin;
                this.state = state;
                this.street = street;
            }

            public Address(int doornumber, String doorpin, String street) {
                this.doornumber = doornumber;
                this.doorpin = doorpin;
                this.street = street;
            }
        }
        
        public class PersonalData {
            [JsonProperty]
            public Address address;

            [JsonProperty]
            public String firstname;

            [JsonProperty]
            public String lastname;

            [JsonProperty]
            public String nas;

            public PersonalData(){}
            public PersonalData(String firstname, String lastname, String nas, Address address) {
                this.firstname = firstname;
                this.lastname = lastname;
                this.nas = nas;
                this.address = address;
            }

            public override string ToString()
            {
                return "NAS: " + nas;
=======
        public static void Main2()
        {
            Meter? meter = null;

            var meterProviderBuilder = Sdk
                .CreateMeterProviderBuilder()
                .AddMeter("Test")
                .SetResourceBuilder(
                    ResourceBuilder.CreateDefault()
                        .AddService(serviceName: "Test"));

            meterProviderBuilder.AddOtlpExporter((exporterOptions, readerOptions) =>
            {
                readerOptions.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds = 5000;
            });

            using var meterProvider = meterProviderBuilder.Build();

            while (true)
            {
                meter?.Dispose();
                GC.Collect();
                meter = new Meter("Test");
                var rd = new Random();

                meter.CreateObservableGauge(
                    "requests",
                    () => new[]
                    {
                        new Measurement<double>(rd.NextDouble() * rd.Next(100)),
                    },
                    description: "Request per second");

                // will see after couple of minutes that the MetricReader contains a lot of MetricPoint[], even if we dispose the Meter after each iteration
                Thread.Sleep(200);
>>>>>>> develop
            }
        }
        
       public static async Task Main(string[] args)
       {
           AwsKmsDriver.Register();
           FieldEncryptionExecutor.Register();
           
           var config = new StreamConfig<StringSerDes, StringSerDes>{
                ApplicationId = $"test-app",
                BootstrapServers = "pkc-921jm.us-east-2.aws.confluent.cloud:9092",
                SaslUsername = "PWRGWBLA7GVVBPKX",
                SaslPassword = "n6heUWG7SsOPMTcLtawsnpMTMqM7SltOzKfZmjSY7+tsNwoHSVMwRtFiRNKTo2y+",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                Acks = Acks.All,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                SessionTimeoutMs = 45000,
                Logger = LoggerFactory.Create((b) =>
                {
                    b.AddConsole();
                    b.SetMinimumLevel(LogLevel.Information);
                }),
                SchemaRegistryUrl = "https://psrc-k6gvm.us-west-2.aws.confluent.cloud",
                BasicAuthUserInfo = "6MIURHIOLB4UVGVH:h/DLxAL7XqYXxZcdZVGg3YhBnimYUtYPZeZv1/kjaxIEh10ANMfqHV+PHuKW6EMR",
                BasicAuthCredentialsSource = "USER_INFO",
                UseLatestVersion = true,
                AutoRegisterSchemas = false
            };
           
           // fix : https://github.com/confluentinc/confluent-kafka-dotnet/pull/2373
           config.AddConfig("json.deserializer.use.latest.version", false);

           var t = BuildTopology();
            var stream = new KafkaStream(t, config);
            
            Console.CancelKeyPress += (_,_) => {
                stream.Dispose();
           };

            await stream.StartAsync();
       }
        
       private static Topology BuildTopology()
        {
            var builder = new StreamBuilder();

            builder.Table<string, PersonalData, StringSerDes, SchemaJsonSerDes<PersonalData>>("personalData")
                .ToStream()
                .Print(Printed<string, PersonalData>.ToOut());

            
            return builder.Build();
        }
    }
}
