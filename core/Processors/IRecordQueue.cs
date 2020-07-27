using System;
using System.Collections.Generic;
using System.Drawing;
using System.Text;

namespace Streamiz.Kafka.Net.Processors
{
    public interface IRecordQueue<T>
    { 
        int Queue(T item);
        T Poll();
        int Size { get; }
        bool IsEmpty { get; }
        void Clear();
    }
}
