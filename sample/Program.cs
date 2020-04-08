using Confluent.Kafka;
using kafka_stream_core;
using kafka_stream_core.SerDes;
using kafka_stream_core.Stream;
using kafka_stream_core.Table;
using System;
using System.Collections.Generic;
using System.Threading;

namespace sample_stream
{
    class CharSerdes : AbstractSerDes<char>
    {
        public override char Deserialize(byte[] data)
        {
            return BitConverter.ToChar(data, 0);
        }

        public override byte[] Serialize(char data)
        {
            return BitConverter.GetBytes(data);
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            CancellationTokenSource source = new CancellationTokenSource();
            
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";
            config.BootstrapServers = "192.168.56.1:9092";
            config.SaslMechanism = SaslMechanism.Plain;
            config.SaslUsername = "admin";
            config.SaslPassword = "admin";
            config.SecurityProtocol = SecurityProtocol.SaslPlaintext;
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.NumStreamThreads = 1;
            
            StreamBuilder builder = new StreamBuilder();

            //var streams = builder.Stream<string, string>("test")
            //    .Branch((k, v) => v.Length % 2 == 0, (k, v) => v.Length % 2 > 0);
            //streams[0].To("test-output-pair");
            //streams[1].To("test-output-impair");

            // BUG : KTableSourceProcessor not have next processor
            builder.Stream<string, string>("test");

            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) => {
                source.Cancel();
                stream.Close();
            };

            stream.Start(source.Token);
        }
    }
}
