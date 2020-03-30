using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Stream
{
    public interface IValueMapper<V, VR>
    {
        VR apply(V value);
    }

    public class WrappedValueMapper<V, VR> : IValueMapper<V, VR>
    {
        private readonly Func<V, VR> wrappedFunction;

        public WrappedValueMapper(Func<V, VR> function)
        {
            this.wrappedFunction = function;
        }

        public VR apply(V value) => wrappedFunction.Invoke(value);
    }
}
