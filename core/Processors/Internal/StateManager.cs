using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors.Internal
{
    internal interface StateManager
    {
        void Flush();
        void Register(StateStore store, StateRestoreCallback callback);
        void Close();
        StateStore GetStore(string name);
    }
}
