using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using static Streamiz.Kafka.Net.Processors.Internal.ProcessorStateManager;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    /// <summary>
    /// StoreChangelogReader is created and maintained by the stream thread and used for both updating standby tasks and
    /// restoring active tasks.It manages the restore consumer, including its assigned partitions, when to pause / resume
    /// these partitions, etc.
    /// </summary>
    internal class StoreChangelogReader : IChangelogReader
    {
        internal enum ChangelogState
        {
            REGISTERED,
            RESTORING,
            COMPLETED
        }

        internal class ChangelogMetadata
        {
            internal StateStoreMetadata StoreMetadata { get; set; }
            internal ProcessorStateManager StateManager { get; set; }
            internal long TotalRestored { get; set; }
            internal long? RestoreEndOffset { get; set; }
            internal int BufferedLimit { get; set; }
            internal ChangelogState ChangelogState { get; set; }
            internal List<ConsumeResult<byte[], byte[]>> BufferedRecords { get; set; }
        }

        private readonly ILog log = Logger.GetLogger(typeof(StoreChangelogReader));
        private readonly IConsumer<byte[], byte[]> restoreConsumer;
        private readonly IDictionary<TopicPartition, ChangelogMetadata> changelogs;
        private readonly static long DEFAULT_OFFSET_UPDATE_MS = (long)TimeSpan.FromMinutes(5L).TotalMilliseconds;
        private readonly long pollTimeMs;
        private readonly long maxPollRestoringRecords;
        // NOT USED AT TIME
        private readonly long updateOffsetTimeMs;
        private long lastUpdateOffsetSend = 0;

        public StoreChangelogReader(
            IStreamConfig config,
            IConsumer<byte[], byte[]> restoreConsumer)
        {
            this.restoreConsumer = restoreConsumer;

            pollTimeMs = config.PollMs;
            maxPollRestoringRecords = config.MaxPollRestoringRecords;
            updateOffsetTimeMs = config.CommitIntervalMs == int.MaxValue ? DEFAULT_OFFSET_UPDATE_MS : config.CommitIntervalMs;

            changelogs = new Dictionary<TopicPartition, ChangelogMetadata>();
        }

        public bool IsEmpty => !changelogs.Any();

        public IEnumerable<TopicPartition> CompletedChangelogs
            => changelogs.Values
                    .Where(c => c.ChangelogState == ChangelogState.COMPLETED)
                    .Select(c => c.StoreMetadata.ChangelogTopicPartition)
                    .ToList();

        public void Clear()
        {
            foreach (var metadata in changelogs.Values)
                metadata.BufferedRecords.Clear();

            changelogs.Clear();

            restoreConsumer.Unassign();
            restoreConsumer.Unsubscribe();
            restoreConsumer.Dispose();
        }

        public void Register(TopicPartition topicPartition, ProcessorStateManager processorStateManager)
        {
            var storeMetadata = processorStateManager.GetStoreMetadata(topicPartition);
            if (storeMetadata == null)
                throw new StreamsException($"Cannot find the corresponding state store metadata for changelog {topicPartition}");

            var changelogMetadata = new ChangelogMetadata
            {
                StoreMetadata = storeMetadata,
                StateManager = processorStateManager,
                ChangelogState = ChangelogState.REGISTERED,
                RestoreEndOffset = null,
                TotalRestored = 0,
                BufferedLimit = 0,
                BufferedRecords = new List<ConsumeResult<byte[], byte[]>>()
            };

            changelogs.Add(topicPartition, changelogMetadata);
        }

        // Workflow :
        // 1 - Init changelogs if new changelogs was added and needs initialization
        // 2 - If all changelogs is restored, return
        // 3 - If any restoring changelogs, read from restore customer and process them
        public void Restore()
        {
            InitChangelogs(RegisteredChangelogs);

            if (AllChangelogsCompleted)
            {
                log.Debug($"Finished restoring all changelogs {string.Join(",", changelogs.Keys.Select(t => $"[{t.Topic}-{t.Partition}]"))}");
                return;
            }

            var restoringChangelogs = RestoringChangelogs;
            if (restoringChangelogs.Any())
            {
                // TODO : exception behavior
                var records = restoreConsumer.ConsumeRecords(TimeSpan.FromMilliseconds(pollTimeMs), maxPollRestoringRecords);

                BufferedRecords(records);

                foreach (var log in RestoringChangelogs)
                    RestoreChangelog(changelogs[log]);

                 // TODO : maybe log restoration phase
            }
        }

        public void Unregister(IEnumerable<TopicPartition> topicPartitions)
        {
            var revokedPartitions = new List<TopicPartition>();
            var assigmentPartitions = new List<TopicPartition>();

            foreach(var part in topicPartitions)
            {
                if (changelogs.ContainsKey(part))
                {
                    if (!(changelogs[part].ChangelogState == ChangelogState.REGISTERED))
                        revokedPartitions.Add(part);

                    changelogs[part].BufferedRecords.Clear();
                    changelogs.Remove(part);
                }
                else
                    log.Debug($"Changelog partition {part} could not be found," +
                                " it could be already cleaned up during the handling" +
                                " of task corruption and never restore again");
            }

            // Assign old partitions less revoke partitions
            assigmentPartitions.AddRange(restoreConsumer.Assignment);
            assigmentPartitions.RemoveAll(t => revokedPartitions.Contains(t));
            restoreConsumer.Assign(assigmentPartitions);
        }

        #region Private

        private IEnumerable<TopicPartition> RestoringChangelogs =>
            changelogs.Values
                .Where(m => m.ChangelogState == ChangelogState.RESTORING)
                .Select(m => m.StoreMetadata.ChangelogTopicPartition)
                .ToList();

        private IEnumerable<ChangelogMetadata> RegisteredChangelogs =>
            changelogs.Values.Where(m => m.ChangelogState == ChangelogState.REGISTERED).ToList();

        private bool AllChangelogsCompleted => changelogs.Values.All(m => m.ChangelogState == ChangelogState.COMPLETED);

        private ChangelogMetadata GetRestoringMetadata(TopicPartition topicPartition)
        {
            if (changelogs.ContainsKey(topicPartition))
            {
                if (changelogs[topicPartition].ChangelogState != ChangelogState.RESTORING)
                {
                    throw new IllegalStateException($"The corresponding changelog restorer for {topicPartition} has already transited to completed state, this should not happen.");
                }
                else
                    return changelogs[topicPartition];
            }
            else
            {
                throw new IllegalStateException($"The corresponding changelog restorer for {topicPartition} does not exist, this should not happen.");
            }
        }

        private void RestoreChangelog(ChangelogMetadata changelogMetadata)
        {
            var numRecords = changelogMetadata.BufferedLimit;

            if (numRecords > 0)
            {
                var records = changelogMetadata.BufferedRecords.Take(numRecords);
                changelogMetadata.StateManager.Restore(changelogMetadata.StoreMetadata, records);

                if (numRecords >= changelogMetadata.BufferedRecords.Count)
                    changelogMetadata.BufferedRecords.Clear();

                long currentOffset = changelogMetadata.StoreMetadata.Offset.Value;
                log.Debug($"Restored {numRecords} records from " +
                    $"changelog {changelogMetadata.StoreMetadata.Store.Name} " +
                    $"to store {changelogMetadata.StoreMetadata.ChangelogTopicPartition}, " +
                    $"end offset is {(changelogMetadata.RestoreEndOffset.HasValue ? changelogMetadata.RestoreEndOffset.Value : "unknown")}, " +
                    $"current offset is {currentOffset}");

                changelogMetadata.BufferedLimit = 0;
                changelogMetadata.TotalRestored += numRecords;

                // TODO : call trigger batchRestored
            }

            if (HasRestoredToEnd(changelogMetadata))
            {
                log.Info($"Finished restoring changelog {changelogMetadata.StoreMetadata.Store.Name} " +
                    $"to store {changelogMetadata.StoreMetadata.ChangelogTopicPartition} " +
                    $"with a total number of {changelogMetadata.TotalRestored} records");

                changelogMetadata.ChangelogState = ChangelogState.COMPLETED;

                if (!restoreConsumer.Assignment.Contains(changelogMetadata.StoreMetadata.ChangelogTopicPartition))
                    throw new IllegalStateException($"The current assignment {string.Join(",", restoreConsumer.Assignment.Select(t => $"{t.Topic}-{t.Partition}"))} " +
                                $"does not contain the partition {changelogMetadata.StoreMetadata.ChangelogTopicPartition} for pausing.");

                restoreConsumer.Pause(changelogMetadata.StoreMetadata.ChangelogTopicPartition.ToSingle());

                log.Debug($"Paused partition {changelogMetadata.StoreMetadata.ChangelogTopicPartition} from the restore consumer");

                // TODO : call trigger restoredEnd
            }
        }

        private bool HasRestoredToEnd(ChangelogMetadata changelogMetadata)
        {
            long? endOffset = changelogMetadata.RestoreEndOffset;
            if (endOffset == null || endOffset == 0)
                return true;
            else if (!changelogMetadata.BufferedRecords.Any())
            {
                var offset = restoreConsumer.Position(changelogMetadata.StoreMetadata.ChangelogTopicPartition);
                return offset == Offset.Unset || offset >= endOffset;
            }
            else
            {
                return changelogMetadata.BufferedRecords[0].Offset >= endOffset;
            }
        }

        private void BufferedRecords(IEnumerable<ConsumeResult<byte[], byte[]>> records)
        {
            foreach (var record in records)
            {
                var metadata = GetRestoringMetadata(record.TopicPartition);
                if (record.Message.Key == null)
                {
                    log.Warn($"Read changelog record with null key from changelog {record.TopicPartition} at offset {record.Offset}, skipping it for restoration");
                }
                else
                {
                    metadata.BufferedRecords.Add(record);
                    if (metadata.RestoreEndOffset == null || record.Offset < metadata.RestoreEndOffset.Value)
                        metadata.BufferedLimit = metadata.BufferedRecords.Count;
                }
            }
        }

        private void InitChangelogs(IEnumerable<ChangelogMetadata> registeredChangelogs)
        {
            if (!registeredChangelogs.Any())
                return;

            IDictionary<TopicPartition, (Offset, Offset)> endOffsets = OffsetsChangelogs(registeredChangelogs);
        
            foreach (var metadata in registeredChangelogs) {
                if (endOffsets.ContainsKey(metadata.StoreMetadata.ChangelogTopicPartition)) {
                    metadata.RestoreEndOffset = endOffsets[metadata.StoreMetadata.ChangelogTopicPartition].Item2;

                    if(metadata.StoreMetadata.Offset.HasValue && metadata.StoreMetadata.Offset < endOffsets[metadata.StoreMetadata.ChangelogTopicPartition].Item1)
                    {
                        log.Info($"State store {metadata.StoreMetadata.Store.Name} initialized from checkpoint " +
                            $"with offset {metadata.StoreMetadata.Offset} is not longer present " +
                            $"at changelog {metadata.StoreMetadata.ChangelogTopicPartition}." +
                            $"Offset is initialized at offset beginning {endOffsets[metadata.StoreMetadata.ChangelogTopicPartition].Item1}");

                        metadata.StoreMetadata.Offset = endOffsets[metadata.StoreMetadata.ChangelogTopicPartition].Item1;
                    }
                }
            }

            // Assign new set of partitions
            var newPartitions = registeredChangelogs.Select(c => c.StoreMetadata.ChangelogTopicPartition).ToList();
            var topicPartitions = new List<TopicPartition>(restoreConsumer.Assignment);
            topicPartitions.AddRange(newPartitions);
            restoreConsumer.Assign(topicPartitions);

            log.Debug($"Added partitions {string.Join(",", newPartitions.Select(c => $"{c.Topic}-{c.Partition}"))} " +
                $"to the restore consumer, current assignment is {topicPartitions.Select(c => $"{c.Topic}-{c.Partition}")}");

            foreach (var r in registeredChangelogs)
                r.ChangelogState = ChangelogState.RESTORING;

            // Seek each changelog to current offset (beginning if not present)
            foreach (var metadata in registeredChangelogs)
            {
                var offset = metadata.StoreMetadata.Offset.HasValue ? new Offset(metadata.StoreMetadata.Offset.Value + 1) : Offset.Beginning;
                restoreConsumer.Seek(
                    new TopicPartitionOffset(
                        metadata.StoreMetadata.ChangelogTopicPartition,
                        offset));

                if (offset == Offset.Beginning)
                    log.Debug($"Start restoring changelog partition {metadata.StoreMetadata.ChangelogTopicPartition} from the beginning offset to end offset {metadata.RestoreEndOffset}.");
                else
                    log.Debug($"Start restoring changelog partition {metadata.StoreMetadata.ChangelogTopicPartition} from current offset {offset.Value} to end offset {metadata.RestoreEndOffset}.");
            }

            // TODO : call trigger onRestoreStart(...)
        }

        private IDictionary<TopicPartition, (Offset, Offset)> OffsetsChangelogs(IEnumerable<ChangelogMetadata> registeredChangelogs)
        {
            return registeredChangelogs
                .Select(_changelog => {
                    var offsets = restoreConsumer.GetWatermarkOffsets(_changelog.StoreMetadata.ChangelogTopicPartition);
                    return new
                    {
                        TopicPartition = _changelog.StoreMetadata.ChangelogTopicPartition,
                        EndOffset = offsets.High,
                        BeginOffset = offsets.Low

                    };
                })
            .ToDictionary(i => i.TopicPartition, i => (i.BeginOffset, i.EndOffset));
        }

        #endregion

        internal ChangelogMetadata GetMetadata(TopicPartition topicPartition)
            => changelogs.ContainsKey(topicPartition) ? changelogs[topicPartition] : null;
    }
}