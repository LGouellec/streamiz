using kafka_stream_core.Crosscutting;
using kafka_stream_core.Mock.Pipes;
using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kafka_stream_core.Mock
{
    public class TestInputTopic<K, V>
    {
        private readonly IPipeInput pipe;
        private readonly IStreamConfig configuration;
        private readonly ISerDes<K> keySerdes;
        private readonly ISerDes<V> valueSerdes;

        private TestInputTopic()
        {

        }

        internal TestInputTopic(IPipeInput pipe, IStreamConfig configuration, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            this.pipe = pipe;
            this.configuration = configuration;
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;
        }

        private (byte[], byte[]) GetBytes(K key, V value)
        {
            byte[] k = keySerdes != null ? keySerdes.Serialize(key) : configuration.DefaultKeySerDes.SerializeObject(key);
            byte[] v = valueSerdes != null ? valueSerdes.Serialize(value) : configuration.DefaultValueSerDes.SerializeObject(value);
            return (k, v);
        }

        #region Pipe One Input

        private void PipeInput(TestRecord<K, V> record)
        {
            long ts = record.Timestamp.HasValue ? record.Timestamp.Value : DateTime.Now.GetMilliseconds();
            var tuple = GetBytes(record.Key, record.Value);
            pipe.Pipe(tuple.Item1, tuple.Item2, ts);
            pipe.Flush();
        }

        public void PipeInput(V value)
            => PipeInput(new TestRecord<K, V> { Value = value });

        public void PipeInput(V value, long timestamp)
            => PipeInput(new TestRecord<K, V> { Value = value, Timestamp = timestamp });

        public void PipeInput(K key, V value)
            => PipeInput(new TestRecord<K, V> { Value = value, Key = key });

        public void PipeInput(K key, V value, long timestamp)
            => PipeInput(new TestRecord<K, V> { Key = key, Value = value, Timestamp = timestamp });

        #endregion

        #region Pipe List Inputs

        private void PipeInputs(IEnumerable<TestRecord<K,V>> records)
        {
            foreach (var record in records)
            {
                long ts = record.Timestamp.HasValue ? record.Timestamp.Value : DateTime.Now.GetMilliseconds();
                var tuple = GetBytes(record.Key, record.Value);
                pipe.Pipe(tuple.Item1, tuple.Item2, ts);
            }

            pipe.Flush();
        }

        public void PipeInputs(IEnumerable<V> valueInputs)
            => PipeInputs(valueInputs.Select(v => new TestRecord<K, V> { Value = v }));

        public void PipeInputs(IEnumerable<KeyValuePair<K, V>> inputs)
            => PipeInputs(inputs.Select(kv => new TestRecord<K, V> { Value = kv.Value, Key = kv.Key }));

        public void PipeInputs(IEnumerable<KeyValuePair<K,V>> inputs, long timestamp, TimeSpan advance)
        {
            long ts = timestamp;
            var records = new List<TestRecord<K, V>>();
            foreach(var i in inputs)
            {
                var r = new TestRecord<K, V>
                {
                    Key = i.Key,
                    Value = i.Value,
                    Timestamp = ts
                };
                records.Add(r);
                ts += (long)advance.TotalMilliseconds;
            }

            this.PipeInputs(records);
        }

        #endregion
    }
}
