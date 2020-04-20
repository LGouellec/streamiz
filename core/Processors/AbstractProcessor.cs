using System;
using System.Collections.Generic;
using System.Text;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using log4net;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class AbstractProcessor<K, V> : IProcessor<K, V>
    {
        protected ILog log = null;
        protected string logPrefix = "";

        public ProcessorContext Context { get; protected set; }

        public string Name { get; private set; }
        public IList<string> StateStores { get; protected set; }

        public ISerDes<K> KeySerDes => Key is ISerDes<K> ? (ISerDes<K>)Key : null;

        public ISerDes<V> ValueSerDes => Value is ISerDes<V>? (ISerDes<V>)Value : null;

        public ISerDes Key { get; internal set; } = null;

        public ISerDes Value { get; internal set; } = null;

        public IList<IProcessor> Previous { get; private set; } = null;

        public IList<IProcessor> Next { get; private set; } = null;

        #region Ctor

        public AbstractProcessor()
            : this(null, null)
        {

        }

        public AbstractProcessor(string name, IProcessor previous)
            : this(name, previous, null, null)
        {
        }

        public AbstractProcessor(string name, IProcessor previous, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            : this(name, previous, keySerdes, valueSerdes, null)
        {
        }

        public AbstractProcessor(string name, IProcessor previous, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, List<string> stateStores)
        {
            Name = name;
            this.SetPreviousProcessor(previous);
            this.SetNextProcessor(null);
            Key = keySerdes;
            Value = valueSerdes;
            StateStores = stateStores != null ? new List<string>(stateStores) : new List<string>();

            log = Logger.GetLogger(this.GetType());
        }

        #endregion

        public virtual void Close()
        {
            // do nothing
        }

        #region Forward

        public virtual void Forward<K1, V1>(K1 key, V1 value)
        {
            this.log.Debug($"{logPrefix}Forward<{typeof(K1).Name},{typeof(V1).Name}> message with key {key} and value {value} to each next processor");
            foreach (var n in Next)
                if (n is IProcessor<K1, V1>)
                    (n as IProcessor<K1, V1>).Process(key, value);
        }

        public virtual void Forward<K1, V1>(K1 key, V1 value, string name)
        {
            foreach (var n in Next)
            {
                if (n is IProcessor<K1, V1> && n.Name.Equals(name))
                {
                    this.log.Debug($"{logPrefix}Forward<{typeof(K1).Name},{typeof(V1).Name}> message with key {key} and value {value} to processor {name}");
                    (n as IProcessor<K1, V1>).Process(key, value);
                }
            }
        }

        public virtual void Forward(K key, V value)
        {
            this.log.Debug($"{logPrefix}Forward<{typeof(K).Name},{typeof(V).Name}> message with key {key} and value {value} to each next processor");
            foreach (var n in Next)
            {
                if (n is IProcessor<K, V>)
                    (n as IProcessor<K, V>).Process(key, value);
                else
                    n.Process(key, value);
            }
        }

        public virtual void Forward(K key, V value, string name)
        {
            foreach (var n in Next)
            {
                if (n.Name.Equals(name))
                {
                    this.log.Debug($"{logPrefix}Forward<{typeof(K).Name},{typeof(V).Name}> message with key {key} and value {value} to processor {name}");
                    if (n is IProcessor<K, V>)
                        (n as IProcessor<K, V>).Process(key, value);
                    else
                        n.Process(key, value);
                }
            }
        }

        #endregion

        public virtual void Init(ProcessorContext context)
        {
            this.log.Debug($"{logPrefix}Initializing process context");
            this.Context = context;
            foreach (var n in Next)
                n.Init(context);
            this.log.Debug($"{logPrefix}Process context initialized");
        }

        protected void LogProcessingKeyValue(K key, V value) => log.Debug($"{logPrefix}Process<{typeof(K).Name},{typeof(V).Name}> message with key {key} and {value} with record metadata [topic:{Context.RecordContext.Topic}|partition:{Context.RecordContext.Partition}|offset:{Context.RecordContext.Offset}]");

        #region Setter

        internal void SetTaskId(TaskId id)
        {
            logPrefix = $"stream-task[{id.Topic}|{id.Partition}]|processor[{Name}]- ";
        }

        public void SetPreviousProcessor(IProcessor prev)
        {
            if (Previous == null)
                Previous = new List<IProcessor>();

            if (prev != null && prev is IProcessor && !Previous.Contains(prev as IProcessor))
                Previous.Add(prev as IProcessor);
        }

        public void SetNextProcessor(IProcessor next)
        {
            if (Next == null)
                Next = new List<IProcessor>();

            if (next != null && next is IProcessor && !Next.Contains(next as IProcessor))
                Next.Add(next as IProcessor);
        }

        public void SetProcessorName(string name)
        {
            this.Name = name;
        }

        #endregion

        #region Process object

        public void Process(object key, object value)
        {
            if (key != null && key is byte[] && KeySerDes != null)
                key = this.Key.DeserializeObject(key as byte[]);

            if (value != null && value is byte[] && ValueSerDes != null)
                value = this.Value.DeserializeObject(value as byte[]);

            if ((key == null || key is K) && (value == null || value is V))
                this.Process((K)key, (V)value);
        }

        #endregion

        #region Abstract

        public abstract void Process(K key, V value);
        public abstract object Clone();

        #endregion

        public override bool Equals(object obj)
        {
            return obj is AbstractProcessor<K, V> && ((AbstractProcessor<K, V>)obj).Name.Equals(this.Name);
        }

        public override int GetHashCode()
        {
            return this.Name.GetHashCode();
        }
    }
}
