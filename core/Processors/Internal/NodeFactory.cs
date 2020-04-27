using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream.Internal;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    #region Node Factory

    internal abstract class NodeFactory
    {
        public string Name { get; }
        public string[] Previous { get; }

        public NodeFactory(string name, string[] previous)
        {
            Name = name;
            Previous = previous;
        }

        public abstract IProcessor Build();
        public abstract NodeDescription Describe();
    }

    #endregion

    #region SourceNode Factory

    internal class SourceNodeFactory<K, V> : NodeFactory
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

    internal class SinkNodeFactory<K, V> : NodeFactory
    {
        public ITopicNameExtractor<K, V> Extractor { get; }
        public ISerDes<K> KeySerdes { get; }
        public ISerDes<V> ValueSerdes { get; }

        public SinkNodeFactory(string name, string[] previous, ITopicNameExtractor<K, V> topicExtractor, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            : base(name, previous)
        {
            Extractor = topicExtractor;
            KeySerdes = keySerdes;
            ValueSerdes = valueSerdes;
        }

        public override IProcessor Build()
            => new SinkProcessor<K, V>(Name, null, Extractor, KeySerdes, ValueSerdes);

        public override NodeDescription Describe()
            => Extractor is StaticTopicNameExtractor<K, V> ?
            new SinkNodeDescription(Name, ((StaticTopicNameExtractor<K, V>)Extractor).TopicName) :
            new SinkNodeDescription(Name, Extractor?.GetType());
    }

    #endregion

    #region ProcessorNode Factory

    internal class ProcessorNodeFactory<K, V> : NodeFactory
    {
        private readonly IList<string> stateStores = new List<string>();

        public IProcessorSupplier<K, V> Supplier { get; }

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

            processor.SetProcessorName(this.Name);
            foreach(var s in stateStores)
                processor.StateStores.Add(s);

            return processor;
        }

        public override NodeDescription Describe()
            => new ProcessorNodeDescription(this.Name, this.stateStores);
    }

    #endregion
}
