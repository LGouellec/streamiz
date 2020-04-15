using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Processors
{
    public interface IStateStore
    {
        String Name { get; }
        bool Persistent { get; }
        bool IsOpen { get; }
        void Init(ProcessorContext context, IStateStore root);
        void Flush();
        void Close();
    }
}
