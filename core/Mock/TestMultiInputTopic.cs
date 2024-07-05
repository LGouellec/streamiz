using Confluent.Kafka;
using Streamiz.Kafka.Net.Mock.Pipes;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Mock
{
    /// <summary>
    /// <see cref="TestMultiInputTopic{K, V}"/> is used to pipe records to multiples topics in <see cref="TopologyTestDriver"/> and it's NOT THREADSAFE.
    /// To use <see cref="TestMultiInputTopic{K, V}"/> create a new instance via
    /// <see cref="TopologyTestDriver.CreateMultiInputTopic{K, V}(string[])"/>.
    /// In actual test code, you can pipe new record values, keys and values or list of keyvalue pairs.
    /// After writtring messages, you must call <see cref="TestMultiInputTopic{K, V}.Flush"/> to persist them !! 
    /// <example>
    /// Processing messages
    /// <code>
    /// var topic = driver.CreateMultiInputTopic&lt;string, string&gt;("test1", "test2");
    /// topic.PipeInput("test1", "key1", "hello");
    /// topic.PipeInput("test2", "key1", "coucou", new Headers());
    /// topic.Flush();
    /// </code>
    /// </example>
    /// </summary>
    /// <typeparam name="K">Key type</typeparam>
    /// <typeparam name="V">Value type</typeparam>
    public class TestMultiInputTopic<K, V>
    {
        private DateTime? lastDate = null;
        private readonly Dictionary<string, IPipeInput> pipes;
        private readonly IStreamConfig configuration;
        private readonly ISerDes<K> keySerdes;
        private readonly ISerDes<V> valueSerdes;

        internal TestMultiInputTopic(Dictionary<string, IPipeInput> pipes, IStreamConfig configuration, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            this.pipes = pipes;
            this.configuration = configuration;
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;

            if (this.keySerdes != null)
                this.keySerdes.Initialize(new SerDesContext(configuration));
            if (this.valueSerdes != null)
                this.valueSerdes.Initialize(new SerDesContext(configuration));
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
                {
                    return keySerdes.Serialize(key, new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Key, topic));
                }
                else
                {
                    return configuration.DefaultKeySerDes.SerializeObject(key, new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Key, topic));
                }
            }
            else
            {
                return null;
            }
        }

        private byte[] GetValueBytes(string topic, V value)
        {
            if (value != null)
            {
                if (valueSerdes != null)
                {
                    return valueSerdes.Serialize(value, new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic));
                }
                else
                {
                    return configuration.DefaultValueSerDes.SerializeObject(value, new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic));
                }
            }
            else
            {
                return null;
            }
        }


        internal IPipeInput GetPipe(string topic)
            => pipes.ContainsKey(topic) ? pipes[topic] : null;

        internal DateTime LastDate
        {
            get
            {
                if (lastDate == null)
                {
                    lastDate = DateTime.Now;
                }
                else
                {
                    lastDate = lastDate.Value.AddSeconds(1);
                }

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
                pipes[topic].Pipe(tuple.Item1, tuple.Item2, ts, record.Headers);
            }
            else
            {
                throw new ArgumentException($"{topic} doesn't found in this MultiInputTopic !");
            }
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
        /// <param name="headers">headers record</param>
        public void PipeInput(string topic, K key, V value, Headers headers = default)
            => PipeInput(topic, new TestRecord<K, V> { Value = value, Key = key, Headers = headers ?? new Headers() });

        /// <summary>
        /// Send an input record with the given record on the topic and then commit the records.
        /// </summary>
        /// <param name="topic">topic name</param>
        /// <param name="key">key record</param>
        /// <param name="value">value record</param>
        /// <param name="timestamp">Timestamp to record</param>
        /// <param name="headers">headers record</param>
        public void PipeInput(string topic, K key, V value, DateTime timestamp, Headers headers = default)
            => PipeInput(topic, new TestRecord<K, V> { Key = key, Value = value, Timestamp = timestamp, Headers = headers ?? new Headers() });

        #endregion

        #region Pipe Inputs

        /// <summary>
        /// Send multiple input records to specific topic
        /// </summary>
        /// <param name="topic">topic name</param>
        /// <param name="datas">list of key/value records</param>
        public void PipeInputs(string topic, params (K, V)[] datas)
        {
            foreach (var d in datas)
            {
                PipeInput(topic, new TestRecord<K, V> { Value = d.Item2, Key = d.Item1 });
            }
        }

        /// <summary>
        /// Send multiple input records to specific topic
        /// </summary>
        /// <param name="topic">topic name</param>
        /// <param name="datas">list of key/value/headers records</param>
        public void PipeInputs(string topic, params (K, V, Headers)[] datas)
        {
            foreach (var d in datas)
            {
                PipeInput(topic, new TestRecord<K, V> { Value = d.Item2, Key = d.Item1, Headers = d.Item3 });
            }
        }

        #endregion

        /// <summary>
        /// Flush all messages since last flush. After writting messages, please call this function for processing them !
        /// </summary>
        public void Flush()
        {
            foreach (var kp in pipes)
            {
                kp.Value.Flush();
            }
        }
    }
}