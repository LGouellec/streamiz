using System;
using System.Collections.Generic;
using System.Threading;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.SerDes.Internal;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.State.Suppress.Internal
{
    internal class InMemoryTimeOrderedKeyValueChangeBuffer<K, V> : ITimeOrderedKeyValueBuffer<K, V, Change<V>>
    {
        public long NumRecords { get; private set; }
        public long BufferSize { get; private set; } = 0L;
        public long MinTimestamp { get; private set; } = Int64.MaxValue;
        public string Name { get; }
        public bool Persistent => false;
        public bool IsLocally { get; }
        public bool IsOpen { get; private set; }

        private Sensor bufferSizeSensor;
        private Sensor bufferCountSensor;
        
        private string changelogTopic;
        private ProcessorContext context;
        private readonly bool loggingEnabled;
        private ISerDes<K> keySerdes;
        private ChangeSerDes<V> valueSerdes;

        private Dictionary<Bytes, BufferKey> index = new();
        private SortedDictionary<BufferKey, BufferValue> sortedValues = new();
        private HashSet<Bytes> dirtyKeys = new();

        
        public InMemoryTimeOrderedKeyValueChangeBuffer(
            string storeName, 
            bool loggingEnabled,
            ISerDes<K> keySerdes,
            ISerDes<V> valueSerdes)
        {
            Name = storeName;
            this.loggingEnabled = loggingEnabled;
            this.keySerdes = keySerdes;
            this.valueSerdes = new ChangeSerDes<V>(valueSerdes);
        }
        
        internal void SetSerdesIfNull(ISerDes<K> contextKeySerdes, ISerDes<V> contextValueSerdes) {
            keySerdes ??= contextKeySerdes;
            valueSerdes ??= new ChangeSerDes<V>(contextValueSerdes);
        }
        
        
        public bool Put(long timestamp, K key, V value, IRecordContext recordContext)
        {
            throw new NotImplementedException();
        }

        public ValueAndTimestamp<Change<V>> PriorValueForBuffered(K key)
        {
            throw new NotImplementedException();
        }

        public void EvictWhile(Func<bool> predicate, Action<K, Change<V>, IRecordContext> evictHandler)
        {
            throw new NotImplementedException();
        }
        
        public void Init(ProcessorContext context, IStateStore root)
        {
            valueSerdes.Initialize(context.SerDesContext);
            changelogTopic = context.ChangelogFor(Name);
            this.context = context;
            
            context.Register(root, Restore);
            UpdateBufferMetrics();
            IsOpen = true;
        }

        private void Restore(Bytes key, byte[] value, long timestamp)
        {
            throw new NotImplementedException();
        }

        private void UpdateBufferMetrics()
        {
            var now = DateTime.Now.GetMilliseconds();
            bufferSizeSensor.Record(BufferSize, now);
            // bufferCountSensor.Record(, now);
        }

        public void Flush()
        {
            if (loggingEnabled)
            {
                
            }
        }

        public void Close()
        {
            IsOpen = false;
            // clear hashmap
            BufferSize = 0;
            MinTimestamp = Int64.MaxValue;
            UpdateBufferMetrics();
            context.Metrics.RemoveStoreSensors(Thread.CurrentThread.Name, 
                context.Id.ToString(),
                Name);
        }
    }
}