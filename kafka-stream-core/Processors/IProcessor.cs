using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors
{
    internal interface IProcessor
    {
        string Name { get; }
        IList<IProcessor> Previous { get; }
        IList<IProcessor> Next { get; }
        void Start();
        void Stop();
        void Kill();
        void Init(ProcessorContext context);
        void SetPreviousProcessor(IProcessor prev);
        void SetNextProcessor(IProcessor next);
    }

    internal interface IProcessor<K,V> : IProcessor
    {
        ISerDes<K> KeySerDes { get; }
        ISerDes<V> ValueSerDes { get; }
        void Process(K key, V value);
    }
}
