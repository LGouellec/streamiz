using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Table.Internal
{
    internal class Change<T>
    {
        public T OldValue { get; }
        public T NewValue { get; }

        public Change(T old, T @new)
        {
            this.OldValue = old;
            this.NewValue = @new;
        }
    }
}
