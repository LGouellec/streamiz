using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class ProcessorStateManager : IStateManager
    {
        internal class StateStoreMetadata
        {
            internal IStateStore Store { get; set; }
            internal TopicPartition ChangelogTopicPartition { get; set; }
        }

        private static readonly string STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";

        private readonly ILog log;
        private readonly string logPrefix;
        private readonly IDictionary<string, StateStoreMetadata> registeredStores = new Dictionary<string, StateStoreMetadata>();
        private readonly TaskId taskId;
        private readonly IDictionary<string, string> changelogTopics;
        private IDictionary<string, IStateStore> globalStateStores = new Dictionary<string, IStateStore>();

        public IEnumerable<TopicPartition> Partition { get; private set; }

        public IEnumerable<string> StateStoreNames => registeredStores.Keys;

        public ProcessorStateManager(TaskId taskId, IEnumerable<TopicPartition> partition, IDictionary<string, string> changelogTopics)
        {
            log = Logger.GetLogger(typeof(ProcessorStateManager));
            logPrefix = $"stream-task[{taskId.Id}|{taskId.Partition}] ";
            this.taskId = taskId;
            Partition = partition;
            this.changelogTopics = changelogTopics ?? new Dictionary<string, string>();
        }

        public static string StoreChangelogTopic(string applicationId, String storeName)
        {
            return $"{applicationId}-{storeName}{STATE_CHANGELOG_TOPIC_SUFFIX}";
        }

        private bool IsChangelogStateStore(string storeName)
            => changelogTopics.ContainsKey(storeName);

        private TopicPartition GetStorePartition(string storeName)
            => new TopicPartition(changelogTopics[storeName], taskId.Partition);

        #region State Manager IMPL

        public void Flush()
        {
            log.Debug($"{logPrefix}Flushing all stores registered in the state manager");

            foreach (var state in registeredStores)
            {
                log.Debug($"{logPrefix}Flushing store {state.Key}");
                state.Value.Store.Flush();
            }
        }

        public void Register(IStateStore store, StateRestoreCallback callback)
        {
            string storeName = store.Name;
            log.Debug($"{logPrefix}Registering state store {storeName} to its state manager");

            if (registeredStores.ContainsKey(storeName))
            {
                throw new ArgumentException($"{logPrefix} Store {storeName} has already been registered.");
            }

            var metadata = IsChangelogStateStore(storeName) ?
                new StateStoreMetadata
                {
                    Store = store,
                    ChangelogTopicPartition = GetStorePartition(storeName)
                } :
                new StateStoreMetadata
                {
                    Store = store
                };

            registeredStores.Add(storeName, metadata);

            log.Debug($"{logPrefix}Registered state store {storeName} to its state manager");
        }

        public void Close()
        {
            log.Debug($"{logPrefix}Closing its state manager and all the registered state stores");

            foreach( var state in registeredStores)
            {
                log.Debug($"{logPrefix}Closing storage engine {state.Key}");
                state.Value.Store.Close();
            }
        }

        public IStateStore GetStore(string name)
        {
            if (registeredStores.ContainsKey(name))
            {
                return registeredStores[name].Store;
            }
            else
            {
                return null;
            }
        }

        public void RegisterGlobalStateStores(IDictionary<string, IStateStore> globalStateStores)
        {
            this.globalStateStores = globalStateStores;
        }

        public TopicPartition GetRegisteredChangelogPartitionFor(string storeName)
        {
            if (registeredStores.ContainsKey(storeName))
            {
                var metadata = registeredStores[storeName];
                if (metadata.ChangelogTopicPartition != null)
                    return metadata.ChangelogTopicPartition;
                else
                    throw new IllegalStateException(
                        @$"Registered state store {storeName} does not have a registered 
                        changelog partition. 
                        This may happen if logging is disabled for 
                        the state store.");
            }
            else
                throw new IllegalStateException(
                    @$"State store {storeName} for which the registered
                    changelog partition should be retrieved has not
                    been registered");
        }

        #endregion
    }
}