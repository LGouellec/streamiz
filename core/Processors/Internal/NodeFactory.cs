using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream.Internal;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    #region Node Factory

    internal interface INodeFactory
    {
        string Name { get; }
        string[] Previous { get; }

        IProcessor Build();
        NodeDescription Describe();
    }

    internal abstract class NodeFactory : INodeFactory
    {
        public string Name { get; }
        public string[] Previous { get; }

        protected NodeFactory(string name, string[] previous)
        {
            Name = name;
            Previous = previous;
        }

        public abstract IProcessor Build();
        public abstract NodeDescription Describe();
    }

    #endregion

    #region SourceNode Factory

    internal interface ISourceNodeFactory : INodeFactory
    {
        string Topic { get; }
        ITimestampExtractor Extractor { get; }
    }

    internal class SourceNodeFactory<K, V> : NodeFactory, ISourceNodeFactory
    {
        public string Topic { get; }
        public ITimestampExtractor Extractor { get; }
        public ISerDes<K> KeySerdes { get; }
        public ISerDes<V> ValueSerdes { get; }

        public SourceNodeFactory(string name, string topic, ITimestampExtractor timestampExtractor, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            : base(name, new string[0])
        {
            Topic = topic;
            Extractor = timestampExtractor;
            KeySerdes = keySerdes;
            ValueSerdes = valueSerdes;
        }

        public override IProcessor Build()
            => new SourceProcessor<K, V>(Name, Topic, KeySerdes, ValueSerdes, Extractor);

        public override NodeDescription Describe()
            => new SourceNodeDescription(Name, Topic, Extractor?.GetType());
    }

    #endregion

    #region SinkNode Factory
    
    internal interface ISinkNodeFactory : INodeFactory
    {
        public string Topic { get; }
    }

    internal class SinkNodeFactory<K, V> : NodeFactory, ISinkNodeFactory
    {
        public ITopicNameExtractor<K, V> TopicExtractor { get; }
        public IRecordTimestampExtractor<K, V> TimestampExtractor { get; }
        public ISerDes<K> KeySerdes { get; }
        public ISerDes<V> ValueSerdes { get; }
        public IStreamPartitioner<K, V> ProducedPartitioner { get; }

        public string Topic =>
            (TopicExtractor as StaticTopicNameExtractor<K, V>)?.TopicName;

        public SinkNodeFactory(string name, string[] previous, ITopicNameExtractor<K, V> topicExtractor,
            IRecordTimestampExtractor<K, V> timestampExtractor,
            ISerDes<K> keySerdes, 
            ISerDes<V> valueSerdes,
            IStreamPartitioner<K, V> producedPartitioner)
            : base(name, previous)
        {
            TopicExtractor = topicExtractor;
            TimestampExtractor = timestampExtractor;
            KeySerdes = keySerdes;
            ValueSerdes = valueSerdes;
            ProducedPartitioner = producedPartitioner;
        }

        public override IProcessor Build()
            => new SinkProcessor<K, V>(Name, TopicExtractor, TimestampExtractor, KeySerdes, ValueSerdes, ProducedPartitioner);

        public override NodeDescription Describe()
            => TopicExtractor is StaticTopicNameExtractor<K, V> ?
            new SinkNodeDescription(Name, ((StaticTopicNameExtractor<K, V>)TopicExtractor).TopicName) :
            new SinkNodeDescription(Name, TopicExtractor?.GetType());
    }

    #endregion

    #region ProcessorNode Factory

    internal interface IProcessorNodeFactory : INodeFactory
    {
        void AddStateStore(string name);
        IReadOnlyList<string> StateStores { get; }
    }

    internal class ProcessorNodeFactory<K, V> : NodeFactory, IProcessorNodeFactory
    {
        private readonly IList<string> stateStores = new List<string>();

        public IProcessorSupplier<K, V> Supplier { get; }

        public IReadOnlyList<string> StateStores => new ReadOnlyCollection<string>(stateStores);

        public ProcessorNodeFactory(string name, string[] previous, IProcessorSupplier<K, V> supplier)
            : base(name, previous)
        {
            Supplier = supplier;
        }

        public void AddStateStore(string name)
        {
            if (!stateStores.Contains(name))
                stateStores.Add(name);
        }

        public override IProcessor Build()
        {
            var processor = Supplier.Get();

            processor.Name = Name;
            foreach(var s in stateStores)
                processor.StateStores.Add(s);

            return processor;
        }

        public override NodeDescription Describe()
            => new ProcessorNodeDescription(Name, stateStores);
    }

    #endregion
}
