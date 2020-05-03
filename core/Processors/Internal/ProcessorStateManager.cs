using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class ProcessorStateManager : IStateManager
    {
        private static string STATE_CHANGELOG_TOPIC_SUFFIX = "-changelog";

        private readonly ILog log;
        private readonly string logPrefix;
        private readonly IDictionary<string, IStateStore> registeredStores = new Dictionary<string, IStateStore>();
        private IDictionary<string, IStateStore> globalStateStores = new Dictionary<string, IStateStore>();

        public TopicPartition Partition { get; private set; }

        public ProcessorStateManager(TaskId taskId, TopicPartition partition)
        {
            this.log = Logger.GetLogger(typeof(ProcessorStateManager));
            this.logPrefix = $"stream-task[{taskId.Topic}|{taskId.Partition}] ";
            Partition = partition;
        }

        public static string StoreChangelogTopic(string applicationId, String storeName)
        {
            return $"{applicationId}-{storeName}{STATE_CHANGELOG_TOPIC_SUFFIX}";
        }

        #region State Manager IMPL

        public void Flush()
        {
            log.Debug($"{logPrefix}Flushing all stores registered in the state manager");

            foreach (var state in registeredStores)
            {
                log.Debug($"{logPrefix}Flushing store {state.Key}");
                state.Value.Flush();
            }
        }

        public void Register(IStateStore store, StateRestoreCallback callback)
        {
            string storeName = store.Name;
            log.Debug($"{logPrefix}Registering state store {storeName} to its state manager");

            if (registeredStores.ContainsKey(storeName))
            {
                throw new ArgumentException($"{this.logPrefix} Store {storeName} has already been registered.");
            }

            // check that the underlying change log topic exist or not
            // TODO : Changelog topic
            //String topic = storeToChangelogTopic.get(storeName);
            //if (topic != null)
            //{
            //    final TopicPartition storePartition = new TopicPartition(topic, getPartition(topic));

            //    final RecordConverter recordConverter = converterForStore(store);

            //    if (isStandby)
            //    {
            //        log.trace("Preparing standby replica of persistent state store {} with changelog topic {}", storeName, topic);

            //        restoreCallbacks.put(topic, stateRestoreCallback);
            //        recordConverters.put(topic, recordConverter);
            //    }
            //    else
            //    {
            //        final Long restoreCheckpoint = store.persistent() ? initialLoadedCheckpoints.get(storePartition) : null;
            //        if (restoreCheckpoint != null)
            //        {
            //            checkpointFileCache.put(storePartition, restoreCheckpoint);
            //        }
            //        log.trace("Restoring state store {} from changelog topic {} at checkpoint {}", storeName, topic, restoreCheckpoint);

            //        final StateRestorer restorer = new StateRestorer(
            //            storePartition,
            //            new CompositeRestoreListener(stateRestoreCallback),
            //            restoreCheckpoint,
            //            offsetLimit(storePartition),
            //            store.persistent(),
            //            storeName,
            //            recordConverter
            //        );

            //        changelogReader.register(restorer);
            //    }
            //    changelogPartitions.add(storePartition);
            //}

            registeredStores.Add(storeName, store);
        }

        public void Close()
        {
            log.Debug($"{logPrefix}Closing its state manager and all the registered state stores");

            foreach (var state in registeredStores)
            {
                log.Debug($"{logPrefix}Closing storage engine {state.Key}");
                state.Value.Close();
            }
        }

        public IStateStore GetStore(string name)
        {
            if (registeredStores.ContainsKey(name))
                return registeredStores[name];
            else
                return null;
        }

        public void RegisterGlobalStateStores(IDictionary<string, IStateStore> globalStateStores)
        {
            this.globalStateStores = globalStateStores;
        }

        #endregion
    }
}
