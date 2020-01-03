using kafka_stream_core.Operators;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core
{
    public class Topology
    {
        private IOperator operatorChain = null;

        internal IOperator OperatorChain { get => operatorChain; }

        internal Topology(IOperator @operator)
        {
            operatorChain = @operator;
        }
    }
}
