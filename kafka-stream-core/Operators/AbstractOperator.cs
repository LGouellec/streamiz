using System;
using System.Collections.Generic;
using System.Text;
using kafka_stream_core.SerDes;

namespace kafka_stream_core.Operators
{
    internal abstract class AbstractOperator<K,V> : IOperator<K,V>
    {
        protected ContextOperator context;

        public AbstractOperator(string name, IOperator previous) 
            : this(name, previous, null, null)
        {
        }

        public AbstractOperator(string name, IOperator previous, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            Name = name;
            this.SetPreviousOperator(previous);
            this.SetNextOperator(null);
            KeySerDes = keySerdes;
            ValueSerDes = valueSerdes;
        }

        public string Name { get; }
        public IList<IOperator> Previous { get; private set; } = null;
        public IList<IOperator> Next { get; private set; } = null;

        public ISerDes<K> KeySerDes { get; }

        public ISerDes<V> ValueSerDes { get; }
        public void SetPreviousOperator(IOperator prev)
        {
            if (Previous == null)
                Previous = new List<IOperator>();

            if (!Previous.Contains(prev) && prev != null)
                Previous.Add(prev);
        }

        public void SetNextOperator(IOperator next)
        {
            if (Next == null)
                Next = new List<IOperator>();

            if (!Next.Contains(next) && next != null)
                Next.Add(next);
        }

        public abstract void Init(ContextOperator context);

        public abstract void Kill();

        public abstract void Start();

        public abstract void Stop();

        public abstract void Message(K key, V value);

        public override bool Equals(object obj)
        {
            return obj is AbstractOperator<K, V> && ((AbstractOperator<K, V>)obj).Name.Equals(this.Name);
        }

        public override int GetHashCode()
        {
            return this.Name.GetHashCode();
        }

    }
}
