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
        bool ThrowException { get; set; }
        string Name { get; }
        bool IsRunning { get; }
        void Run();
        void Start(CancellationToken token);
        IEnumerable<ITask> ActiveTasks { get;  }

        event ThreadStateListener StateChanged;
    }
}
