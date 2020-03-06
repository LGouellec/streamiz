using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors
{
    public interface StateStore
    {
        String Name { get; }
        bool Persistent { get; }
        bool IsOpen { get; }
        void init(ProcessorContext context, StateStore root);
        void flush();
        void close();
    }
}
