using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Processors
{
    internal interface ISourceProcessor 
    {
        string TopicName { get; }
        ITimestampExtractor Extractor { get; }
    }

    internal class SourceProcessor<K,V> : AbstractProcessor<K, V>, ISourceProcessor
    {
        private readonly string topicName;

        internal SourceProcessor(string name, string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, ITimestampExtractor extractor)
            : base(name, null, keySerdes, valueSerdes)
        {
            this.topicName = topicName;
            this.Extractor = extractor;
        }

        private SourceProcessor(SourceProcessor<K,V> sourceProcessor)
            : this(sourceProcessor.Name, sourceProcessor.TopicName, sourceProcessor.KeySerDes, sourceProcessor.ValueSerDes, sourceProcessor.Extractor)
        {
            this.StateStores = new List<string>(sourceProcessor.StateStores);
        }

        public string TopicName => topicName;

        public ITimestampExtractor Extractor { get; }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);

            if (this.Key == null)
                this.Key = context.Configuration.DefaultKeySerDes;

            if (this.Value == null)
                this.Value = context.Configuration.DefaultValueSerDes;
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);

            foreach (var n in Next)
                if (n is IProcessor<K, V>)
                    ((IProcessor<K, V>)n).Process(key, value);
        }

        public override object Clone()
        {
            SourceProcessor<K, V> source = new SourceProcessor<K, V>(this);
            source.SetProcessorName(this.Name);
            this.CloneRecursiveChild(source, this.Next);
            return source;
        }

        private void CloneRecursiveChild(IProcessor root, IEnumerable<IProcessor> child)
        {
            if (child == null || child.Count() == 0)
                return;
            else
            {
                foreach(var c in child)
                {
                    var processor = c.Clone() as IProcessor;
                    processor.SetProcessorName(c.Name);
                    CloneRecursiveChild(processor, c.Next);
                    root.SetNextProcessor(processor);
                }
            }
        }
    }
}
