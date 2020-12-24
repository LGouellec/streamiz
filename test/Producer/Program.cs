using Confluent.Kafka;
using System;

namespace Producer
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Random rd = new Random(DateTime.Now.Millisecond);

            var producerConfig = new ProducerConfig
            {
                Acks = Acks.All,
                BootstrapServers = "localhost:9093"
            };
            var topic = args.Length > 0 ? args[0] : "test";
            var builder = new ProducerBuilder<String, String>(producerConfig);
            Console.WriteLine($"Writting in {topic} topic");
            Console.WriteLine("Enter exit for stopping producer, or enter KEY:VALUE");
            using (var producer = builder.Build())
            {
                string s = Console.ReadLine();
                while (!s.Contains("exit", StringComparison.InvariantCultureIgnoreCase))
                {
                    string[] r = s.Split(":");
                    var randomInt = rd.Next(0, 100000);
                    Headers headers = new Headers();
                    headers.Add(new Header("random", BitConverter.GetBytes(randomInt)));
                    producer.Produce(topic, new Message<string, string> { 
                        Key = r[0],
                        Value = r[1],
                        Headers = headers
                    }, (d) =>
                    {
                        if (d.Status == PersistenceStatus.Persisted)
                        {
                            Console.WriteLine("Message sent !");
                        }
                    });
                    s = Console.ReadLine();
                }
            }
        }
    }
}
