using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class AbstractProcessor<K, V> : IProcessor<K, V>
    {
        protected ILog log = null;
        protected string logPrefix = "";

        public ProcessorContext Context { get; protected set; }

        public string Name { get; set; }
        public IList<string> StateStores { get; protected set; }

        public ISerDes<K> KeySerDes => Key is ISerDes<K> ? (ISerDes<K>)Key : null;

        public ISerDes<V> ValueSerDes => Value is ISerDes<V> ? (ISerDes<V>)Value : null;

        public ISerDes Key { get; internal set; } = null;

        public ISerDes Value { get; internal set; } = null;

        public IList<IProcessor> Next { get; private set; } = new List<IProcessor>();

        #region Ctor

        protected AbstractProcessor()
            : this(null)
        {

        }

        protected AbstractProcessor(string name)
            : this(name, null, null)
        {
        }

        protected AbstractProcessor(string name, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            : this(name, keySerdes, valueSerdes, null)
        {
        }

        protected AbstractProcessor(string name, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, List<string> stateStores)
        {
            Name = name;
            Key = keySerdes;
            Value = valueSerdes;
            StateStores = stateStores != null ? new List<string>(stateStores) : new List<string>();
            log = Logger.GetLogger(GetType());
        }

        #endregion

        public virtual void Close()
        {
            // do nothing
        }

        #region Forward

        public virtual void Forward<K1, V1>(K1 key, V1 value)
        {
            log.Debug($"{logPrefix}Forward<{typeof(K1).Name},{typeof(V1).Name}> message with key {key} and value {value} to each next processor");
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
                    log.Debug($"{logPrefix}Forward<{typeof(K1).Name},{typeof(V1).Name}> message with key {key} and value {value} to processor {name}");
                    (n as IProcessor<K1, V1>).Process(key, value);
                }
            }
        }

        public virtual void Forward(K key, V value)
        {
            log.Debug($"{logPrefix}Forward<{typeof(K).Name},{typeof(V).Name}> message with key {key} and value {value} to each next processor");
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
                    log.Debug($"{logPrefix}Forward<{typeof(K).Name},{typeof(V).Name}> message with key {key} and value {value} to processor {name}");
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
            log.Debug($"{logPrefix}Initializing process context");
            Context = context;
            foreach (var n in Next)
                n.Init(context);
            log.Debug($"{logPrefix}Process context initialized");
        }

        protected void LogProcessingKeyValue(K key, V value) => log.Debug($"{logPrefix}Process<{typeof(K).Name},{typeof(V).Name}> message with key {key} and {value} with record metadata [topic:{Context.RecordContext.Topic}|partition:{Context.RecordContext.Partition}|offset:{Context.RecordContext.Offset}]");

        #region Setter

        internal void SetTaskId(TaskId id)
        {
            logPrefix = $"stream-task[{id.Topic}|{id.Partition}]|processor[{Name}]- ";
        }

        public void AddNextProcessor(IProcessor next)
        {
            if (next != null && !Next.Contains(next as IProcessor))
                Next.Add(next);
        }

        #endregion

        #region Process object

        public void Process(object key, object value)
        {
            bool throwException = false;

            if (key != null && key is byte[])
            {
                if (KeySerDes != null)
                    key = Key.DeserializeObject(key as byte[]);
                else
                    throwException = true;
            }

            if (value != null && value is byte[])
            {
                if (ValueSerDes != null)
                    value = Value.DeserializeObject(value as byte[]);
                else
                    throwException = true;
            }

            if (throwException)
            {
                var s = KeySerDes == null ? "key" : "value";
                log.Error($"{logPrefix}Impossible to receive source data because keySerdes and/or valueSerdes is not setted ! KeySerdes : {(KeySerDes != null ? KeySerDes.GetType().Name : "NULL")} | ValueSerdes : {(ValueSerDes != null ? ValueSerDes.GetType().Name : "NULL")}.");
                throw new StreamsException($"{logPrefix}The {s} serdes is not compatible to the actual {s} for this processor. Change the default {s} serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
            }
            else if ((key == null || key is K) && (value == null || value is V))
                Process((K)key, (V)value);
        }

        #endregion

        #region Abstract

        public abstract void Process(K key, V value);

        #endregion

        public override bool Equals(object obj)
        {
            return obj is AbstractProcessor<K, V> && ((AbstractProcessor<K, V>)obj).Name.Equals(Name);
        }

        public override int GetHashCode()
        {
            return Name.GetHashCode();
        }
    }
}
