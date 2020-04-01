using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors.Internal
{
    internal interface IStateManager
    {
        void Flush();
        void Register(IStateStore store, StateRestoreCallback callback);
        void Close();
        IStateStore GetStore(string name);
    }
}
