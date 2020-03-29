using System;
using System.Collections.Generic;
using System.Text;
using kafka_stream_core.SerDes;

namespace kafka_stream_core.Processors
{
    internal abstract class AbstractProcessor<K, V> : IProcessor<K, V>
    {
        public ProcessorContext Context { get; protected set; }

        public string Name { get; private set; }
        public IList<string> StateStores { get; protected set; }

        public ISerDes<K> KeySerDes { get; }

        public ISerDes<V> ValueSerDes { get; }

        public ISerDes Key => KeySerDes;

        public ISerDes Value => ValueSerDes;

        public IList<IProcessor> Previous { get; private set; } = null;

        public IList<IProcessor> Next { get; private set; } = null;


        public AbstractProcessor()
            : this(null, null)
        {

        }

        public AbstractProcessor(string name, IProcessor previous)
            : this(name, previous, null, null)
        {
        }

        public AbstractProcessor(string name, IProcessor previous, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            Name = name;
            this.SetPreviousProcessor(previous);
            this.SetNextProcessor(null);
            KeySerDes = keySerdes;
            ValueSerDes = valueSerdes;
            StateStores = new List<string>();
        }

        public AbstractProcessor(string name, IProcessor previous, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, List<string> stateStores)
        {
            Name = name;
            this.SetPreviousProcessor(previous);
            this.SetNextProcessor(null);
            KeySerDes = keySerdes;
            ValueSerDes = valueSerdes;
            StateStores = new List<string>(stateStores);
        }

        public virtual void Close()
        {
            // do nothing
        }

        #region Forward

        public virtual void Forward<K1, V1>(K1 key, V1 value)
        {
            foreach (var n in Next)
                if (n is IProcessor<K1, V1>)
                    (n as IProcessor<K1, V1>).Process(key, value);
        }

        public virtual void Forward<K1, V1>(K1 key, V1 value, string name)
        {
            foreach (var n in Next)
                if (n is IProcessor<K1, V1> && n.Name.Equals(name))
                    (n as IProcessor<K1, V1>).Process(key, value);
        }

        public virtual void Forward(K key, V value)
        {
            foreach (var n in Next)
                n.Process(key, value);
        }

        public virtual void Forward(K key, V value, string name)
        {
            foreach (var n in Next)
                if (n.Name.Equals(name))
                    n.Process(key, value);
        }

        #endregion

        public virtual void Init(ProcessorContext context)
        {
            this.Context = context;
            foreach (var n in Next)
                n.Init(context);
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

        public void Process(object key, object value)
        {
            if (key is byte[] && key != null && KeySerDes != null)
                key = this.Key.DeserializeObject(key as byte[]);

            if (value is byte[] && value != null && ValueSerDes != null)
                value = this.Value.DeserializeObject(value as byte[]);

            if ((key == null || key is K) && (value == null || value is V))
                this.Process((K)key, (V)value);
        }


        public abstract void Process(K key, V value);
        // TODO : MUST BE ABSTRACT FOR SUBTOPOLOGY
        public virtual object Clone() { return null; }


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
