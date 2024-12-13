﻿using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text.RegularExpressions;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class InternalTopologyBuilder
    {
        internal class TopologyTopicsInfo
        {
            public IEnumerable<string> SinkTopics { get; internal set; }
            public IEnumerable<string> SourceTopics { get; internal set; }
            public IDictionary<string, InternalTopicConfig> ChangelogTopics { get; internal set; }
            public IDictionary<string, InternalTopicConfig> RepartitionTopics { get; internal set; }
        }

        private readonly IDictionary<string, NodeFactory> nodeFactories = new Dictionary<string, NodeFactory>();
        private readonly IDictionary<string, StateStoreFactory> stateFactories = new Dictionary<string, StateStoreFactory>();
        private readonly IDictionary<string, IStoreBuilder> globalStateBuilders = new Dictionary<string, IStoreBuilder>();
        private readonly IList<string> sourceTopics = new List<string>();
        private readonly IList<string> requestTopics = new List<string>();
        private readonly ISet<string> globalTopics = new HashSet<string>();
        private readonly IDictionary<string, int?> internalTopics = new Dictionary<string, int?>();

        private readonly QuickUnion<string> nodeGrouper = new QuickUnion<string>();
        private IDictionary<int, ISet<string>> nodeGroups = new Dictionary<int, ISet<string>>();
        private string applicationId;
        private readonly IList<ISet<string>> copartitionSourceGroups = new List<ISet<string>>();

        // map from state store names to this state store's corresponding changelog topic if possible
        private readonly IDictionary<string, string> storesToTopics = new Dictionary<string, string>();
        // map from changelog topic name to its corresponding state store.
        private readonly IDictionary<string, string> topicsToStores = new Dictionary<string, string>();

        internal IEnumerable<string> GetSourceTopics()
        {
            var sourceTopicsTmp = new List<string>();
            foreach(var s in sourceTopics)
                sourceTopicsTmp.Add(internalTopics.ContainsKey(s) ? DecorateTopic(s) : s);
            return sourceTopicsTmp;
        }

        internal IEnumerable<string> GetRequestTopics()
            => new ReadOnlyCollection<string>(requestTopics.Select(DecorateTopic).ToList());

        internal IEnumerable<string> GetGlobalTopics() => globalTopics;
        
        internal IDictionary<string, IStateStore> GlobalStateStores { get; } = new Dictionary<string, IStateStore>();

        internal bool HasNoNonGlobalTopology => !sourceTopics.Any();

        internal bool ExternalCall { get; private set; } = false;

        #region Connect

        internal void CopartitionSources(ISet<string> allSourceNodes)
        {
            copartitionSourceGroups.Add(allSourceNodes);
        }

        internal void ConnectProcessorAndStateStore(string processorName, params string[] stateStoreNames)
        {
            foreach (var stateStoreName in stateStoreNames)
            {
                if (globalStateBuilders.ContainsKey(stateStoreName))
                {
                    throw new TopologyException($"Global StateStore {stateStoreName} can be used by a " +
                        "Processor without being specified; it should not be explicitly passed.");
                }
                if (!stateFactories.ContainsKey(stateStoreName))
                {
                    throw new TopologyException("StateStore " + stateStoreName + " is not added yet.");
                }
                if (!nodeFactories.ContainsKey(processorName))
                {
                    throw new TopologyException("Processor " + processorName + " is not added yet.");
                }

                var stateFactory = stateFactories[stateStoreName];
                foreach(var u in stateFactory.users)
                {
                    nodeGrouper.Unite(u, processorName);
                }

                stateFactory.users.Add(processorName);
                var nodeFactory = nodeFactories[processorName];

                if (nodeFactory is IProcessorNodeFactory)
                {
                    ((IProcessorNodeFactory)nodeFactory).AddStateStore(stateStoreName);
                }
                else
                {
                    throw new TopologyException($"Cannot connect a state store {stateStoreName} to a source node or a sink node.");
                }
            }
        }

        internal void ConnectSourceStoreAndTopic(string sourceStoreName, string topic)
        {
            if (storesToTopics.ContainsKey(sourceStoreName))
            {
                throw new TopologyException($"Source store {sourceStoreName} is already added.");
            }

            storesToTopics.AddOrUpdate(sourceStoreName, topic);
            topicsToStores.AddOrUpdate(topic, sourceStoreName);
        }

        #endregion

        #region Add Processors / State Store

        internal void AddSourceOperator<K, V>(string topic, string nameNode, ConsumedInternal<K, V> consumed, bool requestResponsePattern = false)
        {
            if (string.IsNullOrEmpty(topic))
            {
                throw new TopologyException("You must provide at least one topic");
            }

            if (nodeFactories.ContainsKey(nameNode))
            {
                throw new TopologyException($"Source processor {nameNode} is already added.");
            }

            if ((!requestResponsePattern && sourceTopics.Contains(topic)) || 
                (requestResponsePattern && requestTopics.Contains(topic)))
            {
                throw new TopologyException($"Topic {topic} has already been registered by another source.");
            }

            if(!requestResponsePattern)
                sourceTopics.Add(topic);
            else
            {
                requestTopics.Add(topic);
                ExternalCall = true;
            }

            nodeFactories.Add(nameNode,
                new SourceNodeFactory<K, V>(nameNode, topic, consumed.TimestampExtractor, consumed.KeySerdes, consumed.ValueSerdes));
            nodeGrouper.Add(nameNode);
            nodeGroups = null;
        }

        internal void AddSinkOperator<K, V>(ITopicNameExtractor<K, V> topicNameExtractor, IRecordTimestampExtractor<K, V> timestampExtractor, string nameNode, Produced<K, V> produced, params string[] previousProcessorNames)
        {
            if (nodeFactories.ContainsKey(nameNode))
            {
                throw new TopologyException($"Sink processor {nameNode} is already added.");
            }

            nodeFactories.Add(nameNode,
                new SinkNodeFactory<K, V>(nameNode, previousProcessorNames, topicNameExtractor, timestampExtractor, produced.KeySerdes, produced.ValueSerdes, produced.Partitioner));
            nodeGrouper.Add(nameNode);
            nodeGrouper.Unite(nameNode, previousProcessorNames);
            nodeGroups = null;
        }

        internal void AddProcessor<K, V>(string nameNode, IProcessorSupplier<K, V> processor, params string[] previousProcessorNames)
        {
            if (nodeFactories.ContainsKey(nameNode))
            {
                throw new TopologyException($"Processor {nameNode} is already added.");
            }

            nodeFactories.Add(nameNode, new ProcessorNodeFactory<K, V>(nameNode, previousProcessorNames, processor));
            nodeGrouper.Add(nameNode);
            nodeGrouper.Unite(nameNode, previousProcessorNames);
            nodeGroups = null;
        }

        internal void AddStateStore(IStoreBuilder storeBuilder, params string[] processorNames)
        {
            AddStateStore(storeBuilder, false, processorNames);
        }

        internal void AddStateStore(IStoreBuilder storeBuilder, bool allowOverride, params string[] processorNames)
        {
            if (!allowOverride && stateFactories.ContainsKey(storeBuilder.Name))
            {
                throw new TopologyException("StateStore " + storeBuilder.Name + " is already added.");
            }

            stateFactories.Add(storeBuilder.Name, new StateStoreFactory(storeBuilder));

            if (processorNames != null)
            {
                foreach (var processorName in processorNames)
                {
                    ConnectProcessorAndStateStore(processorName, storeBuilder.Name);
                }
            }
        }

        internal void AddGlobalStore<K, V, S>(string topicName,
            IStoreBuilder<S> storeBuilder,
            string sourceName,
            ConsumedInternal<K, V> consumed,
            ProcessorParameters<K, V> processorParameters) where S : IStateStore
        {
            string processorName = processorParameters.ProcessorName;

            ValidateGlobalStoreArguments(sourceName, topicName, processorName, processorParameters.Processor, storeBuilder.Name, storeBuilder.LoggingEnabled);
            ValidateTopicNotAlreadyRegistered(topicName);

            var predecessors = new[] { sourceName };

            var nodeFactory = new ProcessorNodeFactory<K, V>(processorName, predecessors, processorParameters.Processor);

            globalTopics.Add(topicName);
            nodeFactories.Add(sourceName, new SourceNodeFactory<K, V>(sourceName, topicName, consumed.TimestampExtractor, consumed.KeySerdes, consumed.ValueSerdes));

            // TODO: ?
            // nodeToSourceTopics.put(sourceName, Arrays.asList(topics));
            nodeGrouper.Add(sourceName);
            nodeFactory.AddStateStore(storeBuilder.Name);
            nodeFactories.Add(processorName, nodeFactory);
            nodeGrouper.Add(processorName);
            nodeGrouper.Unite(processorName, predecessors);
            globalStateBuilders.Add(storeBuilder.Name, storeBuilder);
            ConnectSourceStoreAndTopic(storeBuilder.Name, topicName);
            nodeGroups = null;
        }

        internal void AddInternalTopic(string repartitionTopic, int? numberPartitions)
        {
            if(!internalTopics.ContainsKey(repartitionTopic)) 
                internalTopics.Add(repartitionTopic, numberPartitions);
        }
        
        private void ValidateTopicNotAlreadyRegistered(string topicName)
        {
            if (sourceTopics.Contains(topicName) || globalTopics.Contains(topicName))
            {
                throw new TopologyException("Topic " + topicName + " has already been registered by another source.");
            }

            // TODO: ?
            //for (Pattern pattern : nodeToSourcePatterns.values())
            //{
            //    if (pattern.matcher(topic).matches())
            //    {
            //        throw new TopologyException("Topic " + topic + " matches a Pattern already registered by another source.");
            //    }
            //}
        }

        private void ValidateGlobalStoreArguments<K, V>(string sourceName,
                                              string topicName,
                                              string processorName,
                                              IProcessorSupplier<K, V> stateUpdateSupplier,
                                              string storeName,
                                              bool loggingEnabled)
        {
            if (nodeFactories.ContainsKey(sourceName))
            {
                throw new TopologyException($"Processor {sourceName} is already added.");
            }
            if (nodeFactories.ContainsKey(processorName))
            {
                throw new TopologyException($"Processor {processorName} is already added.");
            }
            if (stateFactories.ContainsKey(storeName) || globalStateBuilders.ContainsKey(storeName))
            {
                throw new TopologyException("StateStore " + storeName + " is already added.");
            }
            if (loggingEnabled)
            {
                throw new TopologyException($"StateStore {storeName} for global table must not have logging enabled.");
            }
            if (sourceName.Equals(processorName))
            {
                throw new TopologyException("sourceName and processorName must be different.");
            }
        }

        #endregion

        #region Build

        public ProcessorTopology BuildTopology() => BuildTopology((int?)null);

        public ProcessorTopology BuildTopology(int? id)
        {
            ISet<string> nodeGroup = null;
            if (id.HasValue)
            {
                var groups = NodeGroups();
                if (groups.ContainsKey(id.Value))
                {
                    nodeGroup = NodeGroups()[id.Value];
                }
                else
                {
                    throw new TopologyException($"Subtopology {id.Value} doesn't exist in this topology");
                }
            }
            else
            {
                nodeGroup = NodeGroups().Values.SelectMany(i => i).ToHashSet();
            }

            ISet<string> globalNodeGroups = GlobalNodeGroups;
            nodeGroup = nodeGroup.Where(x => !globalNodeGroups.Contains(x)).ToHashSet();

            return BuildTopology(nodeGroup, null);
        }

        public ProcessorTopology BuildGlobalStateTopology()
        {
            if (!GlobalNodeGroups.Any())
            {
                return null;
            }
            return BuildTopology(GlobalNodeGroups, null);
        }

        public ProcessorTopology BuildTopology(TaskId taskId)
        {
            ISet<string> nodeGroup = null;
            if (taskId != null)
            {
                var groups = NodeGroups();
                if (groups.ContainsKey(taskId.Id))
                {
                    nodeGroup = NodeGroups()[taskId.Id];
                }
                else
                {
                    throw new TopologyException($"Task Id {taskId.Id} doesn't exist in this topology");
                }
            }
            else
            {
                nodeGroup = NodeGroups().Values.SelectMany(i => i).ToHashSet();
            }

            return BuildTopology(nodeGroup, taskId);
        }

        private ProcessorTopology BuildTopology(ISet<string> nodeGroup, TaskId taskId)
        {
            // need refactor a little for repartition topic/processor source & sink etc .. change topic name
            IProcessor rootProcessor = new RootProcessor();
            IDictionary<string, IProcessor> sources = new Dictionary<string, IProcessor>();
            IDictionary<string, List<IProcessor>> sinks = new Dictionary<string, List<IProcessor>>();
            IDictionary<string, IProcessor> processors = new Dictionary<string, IProcessor>();
            IDictionary<string, IStateStore> stateStores = new Dictionary<string, IStateStore>();
            IList<string> repartitionTopics = new List<string>();
            
            foreach (var nodeFactory in nodeFactories.Values)
            {
                if (nodeGroup == null || nodeGroup.Contains(nodeFactory.Name))
                {
                    var processor = nodeFactory.Build();
                    processors.Add(nodeFactory.Name, processor);

                    if (nodeFactory is IProcessorNodeFactory)
                    {
                        BuildProcessorNode(processors, stateStores, nodeFactory as IProcessorNodeFactory, processor, taskId);
                    }
                    else if (nodeFactory is ISourceNodeFactory)
                    {
                        BuildSourceNode(sources, repartitionTopics, nodeFactory as ISourceNodeFactory, processor);
                    }
                    else if (nodeFactory is ISinkNodeFactory)
                    {
                        BuildSinkNode(processors, repartitionTopics, sinks, nodeFactory as ISinkNodeFactory, processor);
                    }
                    else
                    {
                        throw new TopologyException($"Unknown definition class: {nodeFactory.GetType().Name}");
                    }
                }
            }

            foreach (var sourceProcessor in sources.Values)
            {
                rootProcessor.AddNextProcessor(sourceProcessor);
            }

            var storesToChangelog = storesToTopics
                            .Where(e => stateStores.ContainsKey(e.Key) 
                                    || GlobalStateStores.ContainsKey(e.Key)).ToDictionary();
            
            return new ProcessorTopology(
                rootProcessor,
                sources,
                sinks,
                processors,
                stateStores,
                GlobalStateStores,
                storesToChangelog,
                repartitionTopics.Distinct().ToList());
        }

        private void BuildSinkNode(
            IDictionary<string, IProcessor> processors,
            IList<string> repartitionTopics,
            IDictionary<string, List<IProcessor>> sinks,
            ISinkNodeFactory factory,
            IProcessor processor)
        {
            foreach (var predecessor in factory.Previous)
            {
                processors[predecessor].AddNextProcessor(processor);
            }
            
            if (factory.Topic != null)
            {
                if (internalTopics.ContainsKey(factory.Topic))
                {
                    var repartitionTopic = DecorateTopic(factory.Topic);
                    repartitionTopics.Add(repartitionTopic);
                    sinks.CreateListOrAdd(repartitionTopic, processor);
                    ((ISinkProcessor) processor).UseRepartitionTopic(repartitionTopic);
                }
                else
                    sinks.CreateListOrAdd(factory.Topic, processor);
            }
            else
                sinks.CreateListOrAdd(factory.Name, processor);
        }

        private void BuildSourceNode(
            IDictionary<string, IProcessor> sources,
            IList<string> repartitionTopics,
            ISourceNodeFactory factory,
            IProcessor processor)
        {
            if (internalTopics.ContainsKey(factory.Topic))
            {
                var repartitionTopic = DecorateTopic(factory.Topic);
                repartitionTopics.Add(repartitionTopic);
                sources.Add(repartitionTopic, processor);
                ((ISourceProcessor) processor).TopicName = repartitionTopic;
            }
            else
                sources.Add(factory.Topic, processor);
        }

        private void BuildProcessorNode(IDictionary<string, IProcessor> processors, IDictionary<string, IStateStore> stateStores, IProcessorNodeFactory factory, IProcessor processor, TaskId taskId)
        {
            foreach (string predecessor in factory.Previous)
            {
                IProcessor predecessorNode = processors[predecessor];
                predecessorNode.AddNextProcessor(processor);
            }

            foreach (string stateStoreName in factory.StateStores)
            {
                if (!stateStores.ContainsKey(stateStoreName))
                {
                    IStateStore store;
                    if (stateFactories.ContainsKey(stateStoreName))
                    {
                        StateStoreFactory stateStoreFactory = stateFactories[stateStoreName];

                        if(stateStoreFactory.LoggingEnabled && !storesToTopics.ContainsKey(stateStoreName))
                        {
                            string changelogTopic = ProcessorStateManager.StoreChangelogTopic(applicationId, stateStoreName);
                            storesToTopics.Add(stateStoreName, changelogTopic);
                        }

                        store = stateStoreFactory.Build(taskId);
                        stateStores.Add(stateStoreName, store);
                    }
                    else
                    {
                        store = GlobalStateStores[stateStoreName];
                        stateStores.Add(stateStoreName, store);
                    }

                    if (store.Persistent)
                        HasPersistentStores = true;
                }
            }
        }

        internal void RewriteTopology(IStreamConfig config)
        {
            foreach (var storeBuilder in globalStateBuilders.Values)
            {
                GlobalStateStores.Add(storeBuilder.Name, storeBuilder.Build());
            }
            
            applicationId = config.ApplicationId;
            config.DefaultKeySerDes?.Initialize(new SerDesContext(config));
            config.DefaultValueSerDes?.Initialize(new SerDesContext(config));
        }

        internal void BuildTopology(RootNode root, IList<StreamGraphNode> nodes)
        {
            foreach (var node in nodes)
            {
                if (node.AllParentsWrittenToTopology && !node.HasWrittenToTopology)
                {
                    node.WriteToTopology(this);
                    node.HasWrittenToTopology = true;
                }
            }
        }

        private ISet<string> GlobalNodeGroups => nodeGroups
                                                    .Where(group => group.Value.Any(IsGlobalSource))
                                                    .SelectMany(group => group.Value)
                                                    .ToHashSet();

        public bool HasPersistentStores { get; private set; } = false;

        private bool IsGlobalSource(string node)
        {
            var factory = nodeFactories[node];
            if (factory is ISourceNodeFactory)
            {
                return globalTopics.Contains(((ISourceNodeFactory)factory).Topic);
            }
            return false;
        }


        #endregion

        #region Make Groups

        internal IDictionary<int, ISet<string>> NodeGroups()
        {
            if (nodeGroups == null)
            {
                nodeGroups = MakeNodeGroups();
            }

            return nodeGroups;
        }

        private IDictionary<int, ISet<string>> MakeNodeGroups()
        {
            IDictionary<int, ISet<string>> groups = new Dictionary<int, ISet<string>>();
            IDictionary<string, ISet<string>> rootToNodeGroup = new Dictionary<string, ISet<string>>();

            int nodeGroupId = 0;

            foreach (var nodeName in nodeFactories.Keys)
            {
                nodeGroupId = PutNodeGroupName(nodeName, nodeGroupId, groups, rootToNodeGroup);
            }

            return groups;
        }

        private int PutNodeGroupName(string nodeName, int nodeGroupId, IDictionary<int, ISet<string>> groups, IDictionary<string, ISet<string>> rootToNodeGroup)
        {
            int newNodeGroupId = nodeGroupId;
            string root = nodeGrouper.Root(nodeName);
            ISet<string> nodeGroup = rootToNodeGroup.ContainsKey(root) ? rootToNodeGroup[root] : null;
            if (nodeGroup == null)
            {
                nodeGroup = new HashSet<string>();
                rootToNodeGroup.Add(root, nodeGroup);
                groups.Add(newNodeGroupId++, nodeGroup);
            }
            nodeGroup.Add(nodeName);
            return newNodeGroupId;
        }

        internal IDictionary<int, TopologyTopicsInfo> MakeInternalTopicGroups()
        {
            IDictionary<int, TopologyTopicsInfo> topicGroups = new Dictionary<int, TopologyTopicsInfo>();

            var nodeGroups = NodeGroups();

            foreach(var entry in nodeGroups)
            {
                List<string> sinkTopics = new List<string>();
                List<string> sourceTopics = new List<string>();
                Dictionary<string, InternalTopicConfig> changelogTopics = new Dictionary<string, InternalTopicConfig>();
                Dictionary<string, InternalTopicConfig> repartitionTopics = new Dictionary<string, InternalTopicConfig>();

                string AddRepartitionTopic(string topic)
                {
                    var internalTopic = DecorateTopic(topic);
                    int? internalTopicPartition = internalTopics[topic];

                    if (internalTopicPartition.HasValue)
                        repartitionTopics.AddIfNotExist(internalTopic,
                            new RepartitionTopicConfig()
                                {Name = internalTopic, NumberPartitions = internalTopicPartition.Value});
                    else
                        repartitionTopics.AddIfNotExist(internalTopic,
                            new RepartitionTopicConfig() {Name = internalTopic});
                    return internalTopic;
                }

                foreach(var node in entry.Value)
                {
                    var nodeFactory = nodeFactories[node];
                    switch (nodeFactory)
                    {
                        case ISourceNodeFactory sourceNodeFactory:
                            if (internalTopics.ContainsKey(sourceNodeFactory.Topic))
                            {
                                string internalTopic = AddRepartitionTopic(sourceNodeFactory.Topic);
                                sourceTopics.Add(internalTopic);
                            }
                            else
                                sourceTopics.Add(sourceNodeFactory.Topic);
                            break;
                        case ISinkNodeFactory sinkNodeFactory:
                            if (sinkNodeFactory.Topic != null && internalTopics.ContainsKey(sinkNodeFactory.Topic))
                            {
                                string internalTopic = AddRepartitionTopic(sinkNodeFactory.Topic);
                                sinkTopics.Add(internalTopic);
                            }
                            else
                                sinkTopics.AddIfNotNull(sinkNodeFactory.Topic);
                            break;
                        case IProcessorNodeFactory processorNodeFactory:
                            foreach(var store in processorNodeFactory.StateStores)
                            {
                                if (storesToTopics.ContainsKey(store))
                                {
                                    var changelogTopic = storesToTopics[store];
                                    if (!changelogTopics.ContainsKey(changelogTopic))
                                    {
                                        if (stateFactories.ContainsKey(store))
                                        {
                                            var internalTopicConfig = CreateChangelogTopicConfig(stateFactories[store], changelogTopic);
                                            changelogTopics.Add(changelogTopic, internalTopicConfig);
                                        }
                                    }
                                }
                            }
                            break;
                    }
                }

                if (sourceTopics.Any())
                {
                    topicGroups.Add(entry.Key, new TopologyTopicsInfo
                    {
                        ChangelogTopics = changelogTopics,
                        RepartitionTopics = repartitionTopics,
                        SinkTopics = sinkTopics,
                        SourceTopics = sourceTopics
                    });
                }
            }

            return topicGroups;
        }

        #endregion

        #region Describe

        internal ITopologyDescription Describe()
        {
            var topologyDes = new TopologyDescription();

            foreach (var kp in NodeGroups())
            {
                bool containsGlobalStore = kp.Value.Any(v => IsGlobalSource(v));
                if (!containsGlobalStore)
                {
                    DescribeSubTopology(topologyDes, kp.Key, kp.Value);
                }
                else
                {
                    DescribeGlobalStore(topologyDes, kp.Value, kp.Key);
                }
            }

            return topologyDes;
        }

        private void DescribeSubTopology(TopologyDescription description, int key, ISet<string> values)
        {
            IDictionary<string, NodeDescription> nodesByName = new Dictionary<string, NodeDescription>();
            foreach (var name in values)
            {
                nodesByName.Add(name, nodeFactories[name].Describe());
            }

            foreach (var node in nodesByName.Values)
            {
                foreach (var prev in nodeFactories[node.Name].Previous)
                {
                    var prevNode = nodesByName[prev];
                    node.AddPredecessor(prevNode);
                    prevNode.AddSuccessor(node);
                }
            }

            description.AddSubtopology(new SubTopologyDescription(key, nodesByName.Values.ToList<INodeDescription>()));
        }

        private void DescribeGlobalStore(TopologyDescription description, ISet<string> values, int key)
        {
            var enumerator = values.GetEnumerator();
            while (enumerator.MoveNext())
            {
                string node = enumerator.Current;
                if (IsGlobalSource(node))
                {
                    enumerator.MoveNext();
                    string processorNode = enumerator.Current;
                    description.AddGlobalStore(
                        new GlobalStoreDescription(
                            node,
                            processorNode,
                            ((IProcessorNodeFactory)nodeFactories[processorNode]).StateStores[0],
                            ((ISourceNodeFactory)nodeFactories[node]).Topic,
                            key)
                        );
                    break;
                }
            }
        }

        #endregion

        private InternalTopicConfig CreateChangelogTopicConfig(StateStoreFactory stateStoreFactory, string changelogTopic)
        {
            if (stateStoreFactory.IsWindowStore)
            {
                var windowChangelogTopicConfig = new WindowedChangelogTopicConfig
                {
                    Configs = stateStoreFactory.LogConfig,
                    Name = changelogTopic,
                    RetentionMs = stateStoreFactory.RetentionMs
                };
                return windowChangelogTopicConfig;
            }

            var unknownChangelogTopicConfig = new UnwindowedChangelogTopicConfig
            {
                Configs = stateStoreFactory.LogConfig,
                Name = changelogTopic,
            };
            return unknownChangelogTopicConfig;
        }

        private ISubTopologyDescription GetSubTopologyDescription(string topic)
        {
            Func<ISourceNodeDescription, bool> predicate = (source) =>
            {
                if (topic == null)
                    return false;
                bool isRepartitionTopic = Regex.IsMatch(topic, $"{applicationId}-(.*)");
                if (source.Topics.Contains(topic))
                    return true;
                if (isRepartitionTopic)
                    return source.Topics.Contains(topic.Replace($"{applicationId}-", ""));
                return false;
            };
            
            var description = Describe();
            var subTopo =
                description
                    .SubTopologies
                    .FirstOrDefault(sub =>
                        sub
                            .Nodes
                            .OfType<ISourceNodeDescription>()
                            .FirstOrDefault(predicate) != null);
            return subTopo;
        }
        
        internal TaskId GetTaskIdFromPartition(TopicPartition topicPartition)
        {
            var description = Describe();
            var subTopo = GetSubTopologyDescription(topicPartition.Topic);

            if (subTopo != null)
            {
                return new TaskId
                {
                    Id = subTopo.Id,
                    Partition = topicPartition.Partition
                };
            }

            var global =
                description
                    .GlobalStores
                    .FirstOrDefault(g => g.Source.Topics.Contains(topicPartition.Topic));
            if (global != null)
            {
                return new TaskId
                {
                    Id = global.Id,
                    Partition = topicPartition.Partition
                };
            }

            throw new TopologyException($"Topic {topicPartition.Topic} doesn't exist in this topology !");
        }

        private string DecorateTopic(string topic) => $"{applicationId}-{topic}";

        // FOR TESTING
        internal void GetLinkTopics(string topic, IList<string> linkTopics)
        {
            var subTopo = GetSubTopologyDescription(topic);
            if (subTopo != null)
            {
                var sinkNodes = subTopo
                    .Nodes
                    .OfType<ISinkNodeDescription>();

                foreach (var sinkNode in sinkNodes)
                {
                    if (GetSubTopologyDescription(sinkNode.Topic) != null)
                    {
                        var sinkTopic = internalTopics.ContainsKey(sinkNode.Topic)
                            ? DecorateTopic(sinkNode.Topic)
                            : sinkNode.Topic;
                        
                        if (linkTopics.Contains(sinkTopic))
                            return;
                        
                        linkTopics.Add(sinkTopic);
                        GetLinkTopics(sinkTopic, linkTopics);
                    }
                }
            }
        }
    }
}