using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal interface IStateManager
    {
        void Flush();
        void Register(IStateStore store, StateRestoreCallback callback);
        void RegisterGlobalStateStores(IDictionary<string, IStateStore> globalStateStores);
        void Close();
        IStateStore GetStore(string name);
    }
}
