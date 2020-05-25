using Confluent.Kafka;
using System;
using System.Threading;

namespace assignor_tasks
{
    internal class Consumer
    {
        private readonly IConsumer<string, string> consumer;
        private readonly string topic;
        private readonly int timeout;
        private readonly System.Threading.CancellationToken token;
        private readonly Thread thread;

        public Consumer(IConsumer<string, string> c1, string topic, int v, System.Threading.CancellationToken token)
        {
            consumer = c1;
            this.topic = topic;
            timeout = v;
            this.token = token;

            thread = new Thread(Run);
            consumer.Subscribe(topic);
        }

        private void Run()
        {
            var random = new Random();
            while (!token.IsCancellationRequested)
            {
                var result = consumer.Consume(timeout);
                if (consumer.Assignment.Count > 0)
                {
                    if (random.Next(0, 100) >= 99) // P = 1/100
                    {
                        consumer.Unassign();
                        var p = random.Next(0, 8);
                        Console.WriteLine($"Manually consumer {consumer.MemberId} assigned to partition {p}");
                        consumer.Assign(new TopicPartition("test", p));
                    }
                }
            }
        }

        public void Start()
        {
            thread.Start();
        }
    
        public void Dispose()
        {
            consumer.Dispose();
        }
    }
}
