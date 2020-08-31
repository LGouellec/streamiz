
using Streamiz.Kafka.Net.Mock.Pipes;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Streamiz.Kafka.Net.Mock
{
    public class TestMultiInputTopic<K, V>
    {
        private DateTime? lastDate = null;
        private Dictionary<string, IPipeInput> pipes;
        private IStreamConfig configuration;
        private ISerDes<K> keySerdes;
        private ISerDes<V> valueSerdes;

        internal TestMultiInputTopic(Dictionary<string, IPipeInput> pipes, IStreamConfig configuration, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            this.pipes = pipes;
            this.configuration = configuration;
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;
        }

        private (byte[], byte[]) GetBytes(string topic, K key, V value)
        {
            byte[] k = GetKeyBytes(topic, key);
            byte[] v = GetValueBytes(topic, value);
            return (k, v);
        }

        private byte[] GetKeyBytes(string topic, K key)
        {
            if (key != null)
            {
                if (keySerdes != null)
                    return keySerdes.Serialize(key, new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Key, topic));
                else
                    return configuration.DefaultKeySerDes.SerializeObject(key, new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Key, topic));
            }
            else
                return null;
        }

        private byte[] GetValueBytes(string topic, V value)
        {
            if (value != null)
            {
                if (valueSerdes != null)
                    return valueSerdes.Serialize(value, new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic));
                else
                    return configuration.DefaultValueSerDes.SerializeObject(value, new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic));
            }
            else
                return null;
        }


        internal IPipeInput GetPipe(string topic)
            => pipes.ContainsKey(topic) ? pipes[topic] : null;

        internal DateTime LastDate
        {
            get
            {
                if (lastDate == null)
                    lastDate = DateTime.Now;
                else
                    lastDate = lastDate.Value.AddSeconds(1);

                return lastDate.Value;
            }
        }

        #region Pipe One Input

        private void PipeInput(string topic, TestRecord<K, V> record)
        {
            if (pipes.ContainsKey(topic))
            {
                DateTime ts = record.Timestamp.HasValue ? record.Timestamp.Value : LastDate;
                var tuple = GetBytes(topic, record.Key, record.Value);
                pipes[topic].Pipe(tuple.Item1, tuple.Item2, ts);
            }
            else
                throw new ArgumentException($"{topic} doesn't found in this MultiInputTopic !");
        }

        /// <summary>
        /// Send an input record with the given record on the topic and then commit the records.
        /// </summary>
        /// <param name="topic">topic name</param>
        /// <param name="value">value record</param>
        public void PipeInput(string topic, V value)
            => PipeInput(topic, new TestRecord<K, V> { Value = value });

        /// <summary>
        /// Send an input record with the given record on the topic and then commit the records.
        /// </summary>
        /// <param name="topic">topic name</param>
        /// <param name="value">value record</param>
        /// <param name="timestamp">Timestamp to record</param>
        public void PipeInput(string topic, V value, DateTime timestamp)
            => PipeInput(topic, new TestRecord<K, V> { Value = value, Timestamp = timestamp });

        /// <summary>
        /// Send an input record with the given record on the topic and then commit the records.
        /// </summary>
        /// <param name="topic">topic name</param>
        /// <param name="key">key record</param>
        /// <param name="value">value record</param>
        public void PipeInput(string topic, K key, V value)
            => PipeInput(topic, new TestRecord<K, V> { Value = value, Key = key });

        /// <summary>
        /// Send an input record with the given record on the topic and then commit the records.
        /// </summary>
        /// <param name="topic">topic name</param>
        /// <param name="key">key record</param>
        /// <param name="value">value record</param>
        /// <param name="timestamp">Timestamp to record</param>
        public void PipeInput(string topic, K key, V value, DateTime timestamp)
            => PipeInput(topic, new TestRecord<K, V> { Key = key, Value = value, Timestamp = timestamp });

        #endregion

        #region Pipe Inputs

        public void PipeInputs(string topic, params (K,V)[] datas)
        {
            foreach(var d in datas)
                PipeInput(topic, new TestRecord<K, V> { Value = d.Item2, Key = d.Item1 });
        }

        #endregion

        public void Flush()
        {
            foreach (var kp in pipes)
                kp.Value.Flush();
        }
    }
}