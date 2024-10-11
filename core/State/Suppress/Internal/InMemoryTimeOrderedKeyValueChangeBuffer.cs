using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.State.Suppress.Internal
{
    internal class InMemoryTimeOrderedKeyValueChangeBuffer<K, V> : ITimeOrderedKeyValueBuffer<K, V, Change<V>>
    {
        public long NumRecords => index.Count;
        public long BufferSize { get; private set; } = 0L;
        public long MinTimestamp { get; private set; } = Int64.MaxValue;
        public string Name { get; }
        public bool Persistent => false;
        public bool IsLocally => true;
        public bool IsOpen { get; private set; }

        private Sensor bufferSizeSensor;
        private Sensor bufferCountSensor;

        private const string metricScope = "in-memory-suppress";
        private string changelogTopic;
        private ProcessorContext processorContext;
        private readonly bool loggingEnabled;
        private ISerDes<K> keySerdes;
        private ISerDes<V> valueSerdes;
        private Func<Change<V>, Change<byte[]>> fullChangeSerializer;
        private Func<Change<byte[]>, Change<V>> fullChangeDeserializer;

        private Dictionary<Bytes, BufferKey> index = new();
        private SortedDictionary<BufferKey, BufferValue> sortedValues = new(new BufferKeyComparer());
        private HashSet<Bytes> dirtyKeys = new();

        private Func<Bytes, BufferValue, long> ComputeRecordSizeFc => (key, value) =>
        {
            long size = 0L;
            size += 8; // buffer time;
            size += key.Get.Length;
            if (value != null)
                size += value.MemoryEstimatedSize();
            return size;
        };
        
        public InMemoryTimeOrderedKeyValueChangeBuffer(
            string storeName, 
            bool loggingEnabled,
            ISerDes<K> keySerdes,
            ISerDes<V> valueSerdes)
        {
            Name = storeName;
            this.loggingEnabled = loggingEnabled;
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;

            fullChangeSerializer = (change) =>
            {
                byte[] oldBytes = change.OldValue == null
                    ? null
                    : valueSerdes.Serialize(change.OldValue,
                        new SerializationContext(MessageComponentType.Value, changelogTopic));
                byte[] newBytes = change.NewValue == null
                    ? null
                    : valueSerdes.Serialize(change.NewValue,
                        new SerializationContext(MessageComponentType.Value, changelogTopic));
                return new Change<byte[]>(oldBytes, newBytes);
            };

            fullChangeDeserializer = (change) =>
            {
                V oldValue = change.OldValue == null
                    ? default
                    : valueSerdes.Deserialize(change.OldValue,
                        new SerializationContext(MessageComponentType.Value, changelogTopic));
                V newValue = change.NewValue == null
                    ? default
                    : valueSerdes.Deserialize(change.NewValue,
                        new SerializationContext(MessageComponentType.Value, changelogTopic));

                return new Change<V>(oldValue, newValue);
            };
        }
        
        public void SetSerdesIfNull(ISerDes<K> contextKeySerdes, ISerDes<V> contextValueSerdes) {
            keySerdes ??= contextKeySerdes;
            valueSerdes ??= contextValueSerdes;
        }
        
        public bool Put(long timestamp, K key, Change<V> value, IRecordContext recordContext)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));
            if (recordContext == null)
                throw new ArgumentNullException(nameof(recordContext));
            
            var serializedKey = Bytes.Wrap(keySerdes.Serialize(key,
                new SerializationContext(MessageComponentType.Key, changelogTopic)));

            var serializedChange = fullChangeSerializer(value);
            
            var buffered = GetBuffered(serializedKey);
            var serializedPriorValue = buffered == null ? serializedChange.OldValue : buffered.PriorValue;
            
            CleanPut(timestamp, 
                serializedKey, 
                new BufferValue(serializedPriorValue, serializedChange.OldValue, serializedChange.NewValue, recordContext));

            if (loggingEnabled)
                dirtyKeys.Add(serializedKey);
            UpdateBufferMetrics();
            
            return true;
        }

        public ValueAndTimestamp<V> PriorValueForBuffered(K key)
        {
            var serializedKey = Bytes.Wrap(keySerdes.Serialize(key,
                new SerializationContext(MessageComponentType.Key, changelogTopic)));
            
            if (index.ContainsKey(serializedKey))
            {
                var serializedValue = InternalPriorValueForBuffered(serializedKey);
                var deserializedValue = valueSerdes.Deserialize(serializedValue,
                    new SerializationContext(MessageComponentType.Value, changelogTopic));

                return ValueAndTimestamp<V>.Make(deserializedValue, -1);
            }
            
            return null;
        }

        public void EvictWhile(Func<bool> predicate, Action<K, Change<V>, IRecordContext> evictHandler)
        {
            int evictions = 0;
            if (predicate())
            {
                List<BufferKey> keyToRemove = new();

                using var eumerator = sortedValues.GetEnumerator();
                bool @continue = eumerator.MoveNext();
                
                while (@continue && predicate())
                {
                    if (eumerator.Current.Key.Time != MinTimestamp)
                        throw new IllegalStateException(
                            $"minTimestamp [{MinTimestamp}] did not match the actual min timestamp {eumerator.Current.Key.Time}");
                    
                    K key = keySerdes.Deserialize(eumerator.Current.Key.Key.Get,
                        new SerializationContext(MessageComponentType.Key, changelogTopic));
                    BufferValue bufferValue = eumerator.Current.Value;
                    Change<V> value =
                        fullChangeDeserializer(new Change<byte[]>(bufferValue.OldValue, bufferValue.NewValue));
                    
                    evictHandler(key, value, bufferValue.RecordContext);
                    keyToRemove.Add(eumerator.Current.Key);
                    index.Remove(eumerator.Current.Key.Key);

                    if (loggingEnabled)
                        dirtyKeys.Add(eumerator.Current.Key.Key);

                    BufferSize -= ComputeRecordSizeFc(eumerator.Current.Key.Key, bufferValue);
                    
                    ++evictions;
                    
                    @continue = eumerator.MoveNext();
                    MinTimestamp = @continue ? eumerator.Current.Key.Time : long.MaxValue;
                }
                
                if(keyToRemove.Any())
                    sortedValues.RemoveAll(keyToRemove);
            }
            
            if(evictions > 0)
                UpdateBufferMetrics();
        }
        
        public void Init(ProcessorContext context, IStateStore root)
        {
            keySerdes.Initialize(context.SerDesContext);
            valueSerdes.Initialize(context.SerDesContext);
            
            changelogTopic = context.ChangelogFor(Name);
            processorContext = context;

            bufferSizeSensor = StateStoreMetrics.SuppressionBufferSizeSensor(
                context.Id,
                metricScope,
                Name,
                context.Metrics);
            
            bufferCountSensor = StateStoreMetrics.SuppressionBufferCountSensor(
                context.Id,
                metricScope,
                Name,
                context.Metrics);
            
            context.Register(root, Restore);
            UpdateBufferMetrics();
            IsOpen = true;
        }

        public void Flush()
        {
            if (loggingEnabled)
            {
                foreach (var dirtyKey in dirtyKeys)
                {
                    var bufferKey = index[dirtyKey];
                    if (bufferKey == null)
                        LogTombstone(dirtyKey);
                    else
                        LogValue(dirtyKey, bufferKey, sortedValues[bufferKey]);
                }
                dirtyKeys.Clear();
            }
        }

        public void Close()
        {
            IsOpen = false;
            index.Clear();
            sortedValues.Clear();
            dirtyKeys.Clear();
            BufferSize = 0;
            MinTimestamp = Int64.MaxValue;
            UpdateBufferMetrics();
            processorContext.Metrics.RemoveStoreSensors(Thread.CurrentThread.Name, 
                processorContext.Id.ToString(),
                Name);
        }
        
        #region Private
        
        private void Restore(Bytes key, byte[] value, long timestamp)
        {
            if (value == null)
            {
                if (index.ContainsKey(key))
                {
                    var bufferedKey = index[key];
                    index.Remove(key);

                    var bufferValue = sortedValues.Get(bufferedKey);
                    if (bufferValue != null)
                    {
                        sortedValues.Remove(bufferedKey);
                        BufferSize -= ComputeRecordSizeFc(bufferedKey.Key, bufferValue);
                    }

                    if (bufferedKey.Time == MinTimestamp)
                        MinTimestamp = !sortedValues.Any() ? long.MaxValue : sortedValues.First().Key.Time;
                }
            }
            else
            {
                var bufferValueAndTime = ByteBuffer.Build(value, true);
                var bufferValue = BufferValue.Deserialize(bufferValueAndTime);
                var time = bufferValueAndTime.GetLong();
                CleanPut(time, key, bufferValue);
            }
            UpdateBufferMetrics();
        }

        private void UpdateBufferMetrics()
        {
            var now = DateTime.Now.GetMilliseconds();
            bufferSizeSensor.Record(BufferSize, now);
            bufferCountSensor.Record(index.Count, now);
        }

        private void LogValue(Bytes key, BufferKey bufferKey, BufferValue bufferValue)
        {
            var buffer = bufferValue.Serialize(sizeof(long));
            buffer.PutLong(bufferKey.Time);
            
            var array = buffer.ToArray();
            processorContext.Log(Name, key, array, bufferKey.Time);
        }

        private void LogTombstone(Bytes key)
            => processorContext.Log(Name, key, null, DateTime.Now.GetMilliseconds());
        
        private void CleanPut(long time, Bytes key, BufferValue bufferValue)
        {
            BufferKey previousKey = index.Get(key);
            if (previousKey == null)
            {
                BufferKey nextKey = new BufferKey(time, key);
                index.Add(key, nextKey);
                sortedValues.Add(nextKey, bufferValue);
                MinTimestamp = Math.Min(MinTimestamp, time);
                BufferSize += ComputeRecordSizeFc(key, bufferValue);
            }
            else
            {
                BufferValue removedValue = sortedValues.Get(previousKey);
                sortedValues.Remove(previousKey);
                BufferSize =
                    BufferSize
                    + ComputeRecordSizeFc(key, bufferValue)
                    - (removedValue == null ? 0 : ComputeRecordSizeFc(key, removedValue));
            }
        }

        private BufferValue GetBuffered(Bytes key)
        {
            BufferKey bufferKey = index.Get(key);
            return bufferKey == null ? null : sortedValues.Get(bufferKey);
        }
        
        private byte[] InternalPriorValueForBuffered(Bytes key) {
            BufferKey bufferKey = index.Get(key);
            if (bufferKey == null) {
                throw new ArgumentException($"Key [{key}] is not in the buffer.");
            }

            BufferValue bufferValue = sortedValues.Get(bufferKey);
            return bufferValue.PriorValue;
        }
        
        #endregion
    }
}