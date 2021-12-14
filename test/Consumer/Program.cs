using Confluent.Kafka;

namespace Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                GroupId = "test-app",
                BootstrapServers = "192.168.56.1:9092",
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "admin",
                SaslPassword = "admin",
                Debug = "generic, broker, topic, metadata, consumer"
            };

            var builder = new ConsumerBuilder<string, string>(config);
            using (var consumer = builder.Build())
            {
                
            }
        }
    }
}