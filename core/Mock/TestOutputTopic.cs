using Confluent.Kafka;
using kafka_stream_core.Crosscutting;
using kafka_stream_core.Errors;
using kafka_stream_core.Mock.Kafka;
using kafka_stream_core.Mock.Pipes;
using kafka_stream_core.SerDes;
using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kafka_stream_core.Mock
{
    /// <summary>
    /// Not thresafe
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <typeparam name="V"></typeparam>
    public class TestOutputTopic<K, V>
    {
        private readonly IPipeOutput pipe;
        private readonly IStreamConfig configuration;
        private readonly ISerDes<K> keySerdes;
        private readonly ISerDes<V> valueSerdes;
        private readonly ILog log = Logger.GetLogger(typeof(TestOutputTopic<K, V>));

        private TestOutputTopic()
        {

        }

        internal TestOutputTopic(IPipeOutput pipe, IStreamConfig configuration, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            this.pipe = pipe;
            this.configuration = configuration;
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;
        }

        public bool IsEmpty => pipe.IsEmpty;

        public int QueueSize => pipe.Size;

        private TestRecord<K, V> ReadRecord()
        {
            try
            {
                var record = pipe.Read();
                var key = keySerdes != null ? keySerdes.Deserialize(record.Key) : (K)configuration.DefaultKeySerDes.DeserializeObject(record.Key);
                var value = valueSerdes != null ? valueSerdes.Deserialize(record.Value) : (V)configuration.DefaultValueSerDes.DeserializeObject(record.Value);
                return new TestRecord<K, V> { Key = key, Value = value };
            }catch(StreamsException e)
            {
                log.Warn($"{e.Message}");
                return null;
            }
        }

        #region Read 

        public V ReadValue() => this.ReadRecord().Value;

        public ConsumeResult<K, V> ReadKeyValue()
        {
            var r = this.ReadRecord();

            return 
                r != null ? new ConsumeResult<K, V>{
                                Message = new Message<K, V> { Key = r.Key, Value = r.Value, Timestamp = new Timestamp(r.Timestamp.HasValue ? r.Timestamp.Value : DateTime.Now) }
                            } 
                        : null;
        }

        #endregion

        #region Read List

        public IEnumerable<ConsumeResult<K, V>> ReadKeyValueList()
        {
            List<ConsumeResult<K, V>> records = new List<ConsumeResult<K, V>>();
            foreach(var record in pipe.ReadList())
            {
                var key = keySerdes != null ? keySerdes.Deserialize(record.Key) : (K)configuration.DefaultKeySerDes.DeserializeObject(record.Key);
                var value = valueSerdes != null ? valueSerdes.Deserialize(record.Value) : (V)configuration.DefaultValueSerDes.DeserializeObject(record.Value);
                records.Add(new ConsumeResult<K, V>{
                    Message = new Message<K, V> { Key = key, Value = value, Timestamp = new Timestamp(DateTime.Now) }
                });
            }
            return records;
        }

        public IEnumerable<V> ReadValueList()
            => ReadKeyValueList().Select(kv => kv.Value).ToList();

        #endregion
    }
}