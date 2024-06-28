using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Mock.Kafka;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    internal class SyncAdminClient : BasedAdminClient
    {
        private readonly SyncProducer producer;
        private readonly bool _autoCreateTopic;
        private AdminClientConfig config;

        public SyncAdminClient(SyncProducer producer, bool autoCreateTopic)
        {
            this.producer = producer;
            _autoCreateTopic = autoCreateTopic;
        }

        internal void UseConfig(AdminClientConfig config)
        {
            this.config = config;
        }

        public override Handle Handle => throw new NotImplementedException();

        public override string Name { get; }
        

        public override Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics, CreateTopicsOptions options = null)
        {
            return Task.Run(() =>
            {
                foreach (var t in topics)
                    producer.CreateTopic(t.Name);
            });
        }
        

        public override Task<List<DescribeConfigsResult>> DescribeConfigsAsync(IEnumerable<ConfigResource> resources, DescribeConfigsOptions options = null)
        {
            return Task.FromResult(new List<DescribeConfigsResult>
            {
                new DescribeConfigsResult
                {
                    Entries = new Dictionary<string, ConfigEntryResult>
                    {
                        {"num.partitions", new ConfigEntryResult{
                                IsDefault = true,
                                IsReadOnly = true,
                                IsSensitive = false,
                                Name = "num.partitions",
                                Value = "1",
                                Source = ConfigSource.DefaultConfig
                            }
                        }
                    }
                }
            });
        }

        public override void Dispose()
        {
        }

        public override Metadata GetMetadata(string topic, TimeSpan timeout)
        {
            var error = new Error(ErrorCode.NoError);
            var topics = producer.GetAllTopics();
            var brokersMetadata = new List<BrokerMetadata>
            {
                new(1, "localhost", 9092)
            };
            
            if (topics.Contains(topic))
            {
                var partitionsMetadata = new List<PartitionMetadata>
                {
                    new(1, 1, new int[1] { 1 }, new int[1] { 1 }, error)
                };

                var topicMetadata = new TopicMetadata(topic, partitionsMetadata, error);
                
                return new Metadata(brokersMetadata,
                    new List<TopicMetadata>() { topicMetadata },
                    1, "localhost");
            }

            if (_autoCreateTopic)
            {
                // auto create topic if not exist
                producer.CreateTopic(topic);
                return GetMetadata(topic, timeout);
            }
            
            return new Metadata(brokersMetadata,
                new List<TopicMetadata>(),
                1, "localhost");
        }

        public override Metadata GetMetadata(TimeSpan timeout)
        {
            var topicsMetadata = new List<TopicMetadata>();
            var topics = producer.GetAllTopics();
            var error = new Error(ErrorCode.NoError);

            var brokersMetadata = new List<BrokerMetadata> {
                new BrokerMetadata(1, "localhost", 9092)
            };

            foreach(var t in topics)
            {
                var partitionsMetadata = new List<PartitionMetadata> {
                    new PartitionMetadata(0, 0, new int[1]{0}, new int[1]{0}, error)
                };

                topicsMetadata.Add(new TopicMetadata(t, partitionsMetadata, error));
            }

            return new Metadata(brokersMetadata, topicsMetadata, 1, "localhost");
        }
        
    }
}
