using kafka_stream_core.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.State.Internal
{
    internal class WrappedStateStore<S, K, V> : StateStore
        where S : StateStore
    {

        protected readonly S wrapped;

        public WrappedStateStore(S wrapped)
        {
            this.wrapped = wrapped;
        }

        #region StateStore Impl

        public string Name => wrapped.Name;

        public bool Persistent => wrapped.Persistent;

        public bool IsOpen => wrapped.IsOpen;

        public void close() => wrapped.close();

        public void flush() => wrapped.flush();

        public void init(ProcessorContext context, StateStore root) => wrapped.init(context, root);

        #endregion
    }
}
