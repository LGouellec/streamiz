using Confluent.Kafka;
using Streamiz.Kafka.Net.Errors;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    internal class SyncConsumer : IConsumer<byte[], byte[]>
    {
        private ConsumerConfig config;
        private SyncKafkaSupplier ownSupplier;

        private readonly IDictionary<string, long> offsets = new Dictionary<string, long>();

        public SyncConsumer(ConsumerConfig config, SyncKafkaSupplier syncKafkaSupplier)
        {
            this.config = config;
            ownSupplier = syncKafkaSupplier;
        }

        #region IConsumer Impl

        public string MemberId => config.GroupId;

        public List<TopicPartition> Assignment { get; private set; } = new List<TopicPartition>();

        public List<string> Subscription { get; private set; } = new List<string>();

        public IConsumerGroupMetadata ConsumerGroupMetadata => null;

        public Handle Handle => null;

        public string Name => config.ClientId;

        public int AddBrokers(string brokers) => 0;

        public void Assign(TopicPartition partition)
        {
            // TODO
            if (!offsets.ContainsKey(partition.Topic))
                offsets.Add(partition.Topic, 0L);
        }

        public void Assign(TopicPartitionOffset partition)
        {
            // TODO
            if (!offsets.ContainsKey(partition.Topic))
                offsets.Add(partition.Topic, partition.Offset);
            else
                offsets[partition.Topic] = partition.Offset;
        }

        public void Assign(IEnumerable<TopicPartitionOffset> partitions)
        {
            // TODO
            foreach (var p in partitions)
                Assign(p);
        }

        public void Assign(IEnumerable<TopicPartition> partitions)
        {
            // TODO
            foreach (var p in partitions)
                Assign(p);
        }

        public void Close()
        {
            Assignment.Clear();
            Subscription.Clear();
        }

        public List<TopicPartitionOffset> Commit()
        {
            foreach (var kp in offsets)
            {
                var producer = ownSupplier.GetProducerInstance(kp.Key);
                offsets[kp.Key] = producer.GetHistory(kp.Key).Count() + 1;
            }
            return offsets.Select(k => new TopicPartitionOffset(new TopicPartition(k.Key, 0), k.Value)).ToList();
        }

        public void Commit(IEnumerable<TopicPartitionOffset> offsets)
        {
            foreach (var offset in offsets)
            {
                if (this.offsets.ContainsKey(offset.Topic))
                {
                    var producer = ownSupplier.GetProducerInstance(offset.Topic);
                    this.offsets[offset.Topic] = offset.Offset;
                }
                else
                {
                    this.offsets.Add(offset.Topic, offset.Offset);
                }
            }

        }

        public void Commit(ConsumeResult<byte[], byte[]> result)
            => this.Commit(new List<TopicPartitionOffset> { new TopicPartitionOffset(result.TopicPartition, result.Offset+1) });

        public List<TopicPartitionOffset> Committed(TimeSpan timeout)
        {
            List<TopicPartitionOffset> r = new List<TopicPartitionOffset>();
            foreach (var kp in offsets)
                r.Add(new TopicPartitionOffset(new TopicPartition(kp.Key, 0), kp.Value));
            return r;
        }

        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
        {
            // TODO : 
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            Close();
        }

        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
        {
            var producer = ownSupplier.GetProducerInstance(topicPartition.Topic);
            return new WatermarkOffsets(0L, producer.GetHistory(topicPartition.Topic).Count());
        }

        public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
        {
            // TODO : 
            throw new NotImplementedException();
        }

        public void Pause(IEnumerable<TopicPartition> partitions)
        {
            // TODO : 
        }

        public Offset Position(TopicPartition partition)
        {
            // TODO
            throw new NotImplementedException();
        }

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
        {
            // TODO
            throw new NotImplementedException();
        }

        public void Resume(IEnumerable<TopicPartition> partitions)
        {
            // TODO
            throw new NotImplementedException();
        }

        public void Seek(TopicPartitionOffset tpo)
        {
            // TODO
            throw new NotImplementedException();
        }

        public void StoreOffset(TopicPartitionOffset offset)
        {
            // TODO
            throw new NotImplementedException();
        }

        public void StoreOffset(ConsumeResult<byte[], byte[]> result)
        {
            // TODO
            throw new NotImplementedException();
        }

        public void Subscribe(IEnumerable<string> topics)
        {
            Subscription.AddRange(topics);
            foreach (var t in topics)
                if (!offsets.ContainsKey(t))
                    offsets.Add(t, 0L);
        }

        public void Subscribe(string topic)
        {
            Subscription.Add(topic);
            if (!offsets.ContainsKey(topic))
                offsets.Add(topic, 0L);
        }

        public void Unassign()
        {
            Assignment.Clear();
        }

        public void Unsubscribe()
        {
            Subscription.Clear();
        }

        public ConsumeResult<byte[], byte[]> Consume(int millisecondsTimeout)
            => Consume(TimeSpan.FromMilliseconds(millisecondsTimeout));

        public ConsumeResult<byte[], byte[]> Consume(CancellationToken cancellationToken)
        {
            if (this.Subscription.Count == 0)
                throw new StreamsException("No subscription have been done !");

            return ConsumeInternal(TimeSpan.FromSeconds(10));
        }

        public ConsumeResult<byte[], byte[]> Consume(TimeSpan timeout)
        {
            if (this.Subscription.Count == 0)
                throw new StreamsException("No subscription have been done !");

            return ConsumeInternal(timeout);
        }

        private ConsumeResult<byte[], byte[]> ConsumeInternal(TimeSpan timeout)
        {
            DateTime dt = DateTime.Now;
            ConsumeResult<byte[], byte[]> result = null;

            while ((dt + timeout) > DateTime.Now)
            {
                foreach (var kp in offsets)
                {
                    if ((dt + timeout) < DateTime.Now)
                        break;

                    var producer = ownSupplier.GetProducerInstance(kp.Key);
                    if (producer != null) {
                        var messages = producer.GetHistory(kp.Key).ToArray();
                        if (messages.Length > kp.Value)
                        {
                            result = new ConsumeResult<byte[], byte[]>
                            {
                                Offset = kp.Value,
                                Topic = kp.Key,
                                Partition = 0,
                                Message = messages[kp.Value]
                            };
                            return result;
                        }
                    }
                }
            }
            return result;
        }

        #endregion
    }
}
