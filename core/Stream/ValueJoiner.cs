using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Stream
{
    public interface IValueJoiner<in V1, in V2, out VR>
    {
        VR Apply(V1 value1, V2 value2);
    }

    internal class WrappedValueJoiner<V1, V2, VR> : IValueJoiner<V1, V2, VR>
    {
        private readonly Func<V1, V2, VR> function;

        public WrappedValueJoiner(Func<V1, V2,VR> function)
        {
            this.function = function;
        }

        public VR Apply(V1 value1, V2 value2) => function.Invoke(value1, value2);
    }
}
