using System.Collections;
using System.Collections.Generic;
using Confluent.Kafka;

public interface IOffsetCheckpointManager {
    IDictionary<TopicPartition, long> Read();
    void Write(IDictionary<TopicPartition, long> data);
    void Destroy();
}