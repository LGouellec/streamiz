using System;
using System.Collections.Generic;
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
        bool IsRunningAndNotRebalancing { get; }
        void Run();
        void Start(CancellationToken token);
        IEnumerable<ITask> Tasks { get;  }

        event ThreadStateListener StateChanged;
    }
}
