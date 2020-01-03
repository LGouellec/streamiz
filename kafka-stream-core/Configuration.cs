using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core
{
    public class Configuration : Dictionary<string, string>
    {
        private string applicatonIdCst = "application-id";
        public string ApplicationId
        {
            get => this[applicatonIdCst];
            set => Add(applicatonIdCst, value);
        }
      
        public ProducerConfig toProducerConfig()
        {
            return new ProducerConfig
            {
                BootstrapServers = this["bootstrap.servers"],
                SaslMechanism = SaslMechanism.ScramSha512,
                SaslUsername = this["sasl.username"],
                SaslPassword = this["sasl.password"],
                SecurityProtocol = SecurityProtocol.SaslPlaintext
            };
        }

        public ConsumerConfig toConsumerConfig()
        {
            return new ConsumerConfig
            {
                BootstrapServers = this["bootstrap.servers"],
                SaslMechanism = SaslMechanism.ScramSha512,
                SaslUsername = this["sasl.username"],
                SaslPassword = this["sasl.password"],
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                GroupId = this.ApplicationId
            };
        }
    }
}
