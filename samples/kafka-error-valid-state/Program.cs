using Confluent.Kafka;
using System;
using System.Threading;

namespace kafka_error_valid_state
{
    class Program
    {
        static void Main(string[] args)
        {
            int numThread = 5;
            CancellationTokenSource source = new CancellationTokenSource();
            string topic = "test";

            ConsumerConfig config = new ConsumerConfig();
            config.GroupId = "test-app";
            config.BootstrapServers = "localhost:29092";
            config.IsolationLevel = IsolationLevel.ReadCommitted;
            config.MaxInFlight = 5;
            config.EnableAutoCommit = false;
            config.Debug = "consumer,cgrp,topic";


            ConsumerThread[] cs = new ConsumerThread[numThread];

            Console.CancelKeyPress += (o, e) =>
            {
                source.Cancel();
                for (int i = 0; i < numThread; ++i)
                    cs[i].Close();
            };

            for (int i = 0; i < numThread; ++i)
                cs[i] = new ConsumerThread(config, topic, source.Token);

            for (int i = 0; i < numThread; ++i)
                cs[i].Start();

        }
    }
}
