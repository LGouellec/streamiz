using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors
{
    interface IThread : IDisposable
    {
        int Id { get; }
        bool IsDisposable { get; }
        string Name { get; }
        bool IsRunning { get; }
        void Run();
        void Start();
    }
}
