using System;
using System.Threading;

namespace Streamiz.Kafka.Net.Processors
{
    interface IThread : IDisposable
    {
        int Id { get; }
        ThreadState State { get; }
        bool IsDisposable { get; }
        string Name { get; }
        bool IsRunning { get; }
        void Run();
        void Start(CancellationToken token);

        event ThreadStateListener StateChanged;
    }
}
