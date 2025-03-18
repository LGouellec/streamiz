using System;
using System.Diagnostics;
using System.Runtime.InteropServices.JavaScript;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Encryption;
using Confluent.SchemaRegistry.Encryption.Aws;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Json;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace sample_stream
{
    public static class Program
    {
        public class Address
        {
            [JsonProperty] private int doornumber;

            [JsonProperty] private String doorpin;

            [JsonProperty] private String state;

            [JsonProperty] private String street;

            public Address()
            {
            }

            public Address(int doornumber, String doorpin, String state, String street)
            {
                this.doornumber = doornumber;
                this.doorpin = doorpin;
                this.state = state;
                this.street = street;
            }

            public Address(int doornumber, String doorpin, String street)
            {
                this.doornumber = doornumber;
                this.doorpin = doorpin;
                this.street = street;
            }
        }

        public class PersonalData
        {
            [JsonProperty] public Address address;

            [JsonProperty] public String firstname;

            [JsonProperty] public String lastname;

            [JsonProperty] public String nas;

            public PersonalData()
            {
            }

            public PersonalData(String firstname, String lastname, String nas, Address address)
            {
                this.firstname = firstname;
                this.lastname = lastname;
                this.nas = nas;
                this.address = address;
            }

            public override string ToString()
            {
                return "NAS: " + nas;
            }
        }

        public static async Task Main(string[] args)
        {
            /*AwsKmsDriver.Register();
            FieldEncryptionExecutor.Register();

            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = $"test-app",
                BootstrapServers = "localhost:9092",
                Acks = Acks.All,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                SessionTimeoutMs = 45000,
                Logger = LoggerFactory.Create((b) =>
                {
                    b.AddConsole();
                    b.SetMinimumLevel(LogLevel.Information);
                }),
            };
            
            var t = BuildTopology();
            var stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (_, _) => { stream.Dispose(); };

            await stream.StartAsync();*/

            var reproducerProtobuf = new ReproducerProtobuf();
            await reproducerProtobuf.Test();
        }

        private static Topology BuildTopology()
        {
            var builder = new StreamBuilder();

            builder.Stream<string, PersonalData, StringSerDes, SchemaJsonSerDes<PersonalData>>("personalData")
                .MapValues((k, v, r) =>
                {
                    if(r.Headers == null)
                        r.SetHeaders(new Headers());
                    return v;
                });
            //    .Print(Printed<string, PersonalData>.ToOut());

            return builder.Build();
        }
    }

}