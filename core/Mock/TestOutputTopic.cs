using kafka_stream_core.Errors;
using kafka_stream_core.Mock.Kafka;
using kafka_stream_core.Mock.Pipes;
using kafka_stream_core.SerDes;
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

        public bool IsEmpty
        {
            get
            {
                var infos = pipe.GetInfos();
                foreach (var i in infos)
                    if (i.Offset < i.High)
                        return false;
                return true;
            }
        }

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
                return new TestRecord<K, V> { Key = default(K), Value = default(V) };
            }
        }

        #region Read 

        public V ReadValue() => this.ReadRecord().Value;

        public KeyValuePair<K, V> ReadKeyValue()
        {
            var r = this.ReadRecord();
            return new KeyValuePair<K, V>(r.Key, r.Value);
        }

        #endregion

        #region Read List

        public IEnumerable<KeyValuePair<K, V>> ReadKeyValueList()
        {
            List<KeyValuePair<K, V>> records = new List<KeyValuePair<K, V>>();
            while (!IsEmpty)
                records.Add(this.ReadKeyValue());
            return records;
        }

        public IEnumerable<V> ReadValueList()
            => ReadKeyValueList().Select(kv => kv.Value).ToList();

        #endregion
    }
}