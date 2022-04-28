using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Prometheus;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace sample_stream_demo
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string inputTopic = "input", outputTopic = "output";
            int numberPartitions = 4;

            await CreateTopics(inputTopic, outputTopic, numberPartitions);
            
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "sample-streamiz-demo";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.StateDir = Path.Combine(".");
            config.CommitIntervalMs = 5000;
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.MetricsRecording = MetricsRecordingLevel.DEBUG;
            config.UsePrometheusReporter(9090, true);

            StreamBuilder builder = new StreamBuilder();
            
            builder.Stream<string, string>(inputTopic)
                .FlatMapValues((v) => v.Split(" ").AsEnumerable())
                .GroupBy((k,v) => v)
                .Count()
                .ToStream()
                .To(outputTopic);
            
            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);
            
            Console.CancelKeyPress += (o, e) => stream.Dispose();

            await stream.StartAsync();
        }

        private static async Task CreateTopics(string input, string output, int numberPartitions)
        {
            AdminClientConfig config = new AdminClientConfig();
            config.BootstrapServers = "localhost:9092";

            AdminClientBuilder builder = new AdminClientBuilder(config);
            var client = builder.Build();
            try
            {
                await client.CreateTopicsAsync(new List<TopicSpecification>
                {
                    new TopicSpecification() {Name = input, NumPartitions = numberPartitions},
                    new TopicSpecification() {Name = output, NumPartitions = numberPartitions},
                });
            }
            catch (Exception e)
            {
                // do nothing in case of topic already exist
            }
            finally
            {
                client.Dispose();
            }
        }
    }
}