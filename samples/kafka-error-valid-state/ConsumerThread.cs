using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace kafka_error_valid_state
{
    internal class ConsumerThread
    { 
        private readonly Thread _thread;
        private readonly ConsumerConfig confg;
        private readonly string topic;
        private readonly CancellationToken token;

        public ConsumerThread(ConsumerConfig confg, string topic, CancellationToken token)
        {
            this.confg = confg;
            this.topic = topic;
            this.token = token;
            _thread = new Thread(Run);
        }

        private void Run()
        {
            var builder = new ConsumerBuilder<string, string>(confg);
            var consumer = builder.Build();
            consumer.Subscribe(topic);

            while (!token.IsCancellationRequested)
            {
                var record = consumer.Consume(100);
                if (record != null)
                {
                    Console.WriteLine($"[Topic:{record.Topic}|Partition:{record.Partition}|Offset:{record.Offset}] => Key:{record.Message.Key}|Value:{record.Message.Value}");
                    consumer.Commit(record);
                }
            }

            consumer.Dispose();
        }

        internal void Close()
        {
            _thread.Join();
        }

        internal void Start()
        {
            _thread.Start();
        }
    }
}
