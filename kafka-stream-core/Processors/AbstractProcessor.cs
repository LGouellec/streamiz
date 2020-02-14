using System;
using System.Collections.Generic;
using System.Text;
using kafka_stream_core.SerDes;

namespace kafka_stream_core.Processors
{
    internal abstract class AbstractProcessor<K,V> : IProcessor<K,V>
    {
        protected ProcessorContext context;

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
        }

        public string Name { get; }
        public IList<IProcessor> Previous { get; private set; } = null;
        public IList<IProcessor> Next { get; private set; } = null;

        public ISerDes<K> KeySerDes { get; }

        public ISerDes<V> ValueSerDes { get; }
        public void SetPreviousProcessor(IProcessor prev)
        {
            if (Previous == null)
                Previous = new List<IProcessor>();

            if (!Previous.Contains(prev) && prev != null)
                Previous.Add(prev);
        }

        public void SetNextProcessor(IProcessor next)
        {
            if (Next == null)
                Next = new List<IProcessor>();

            if (!Next.Contains(next) && next != null)
                Next.Add(next);
        }

        public void Init(ProcessorContext context)
        {
            foreach (var n in Next)
                n.Init(context);
            this.context = context;
        }

        public virtual void Kill() { }

        public virtual void Start() { }

        public virtual void Stop() { }

        public abstract void Process(K key, V value);

        public void Forward(K key, V value)
        {
            foreach (var n in Next)
                if (n is IProcessor<K, V>)
                    ((IProcessor<K, V>)n).Process(key, value);
        }

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
