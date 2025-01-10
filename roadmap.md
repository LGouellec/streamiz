# Roadmap Feature

- [ ] Dead letter queue mecanism
- [ ] Reafactor External Call with similar behavior like ParrallelConsumer + a real async approach
- [ ] Auto scaling consumption (inspired KEDA)
- [ ] State store restore handler with a batch approach
- [ ] Evict cache store as a batch of records instead of unitary records
- [ ] KIP-612 : end-to-end latency metrics
- [ ] KIP-450 : Sliding windows
- [ ] Session Windows
- [ ] Rename WithRecordTimestamp to AlterRecordTimestamp
- [ ] Versioned State Store
- [ ] https://cwiki.apache.org/confluence/display/KAFKA/KIP-923%3A+Add+A+Grace+Period+to+Stream+Table+Join
- [ ] KIP-424 : Allow suppression of intermediate events based on wall clock time
- [ ] Extensions Streamiz
- [ ] At-most-once processing guarantee
- [ ] Support “broadcast” pattern
- [ ] Replace repartition topics with network shuffles
- [ ] Cross Cluster