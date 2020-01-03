using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Operators
{
    internal interface IOperator
    {
        string Name { get; }
        IList<IOperator> Previous { get; }
        IList<IOperator> Next { get; }
        void Start();
        void Stop();
        void Kill();
        void Init(ContextOperator context);
        void SetPreviousOperator(IOperator prev);
        void SetNextOperator(IOperator next);
    }

    internal interface IOperator<K,V> : IOperator
    {
        ISerDes<K> KeySerDes { get; }
        ISerDes<V> ValueSerDes { get; }
        void Message(K key, V value);
    }
}
