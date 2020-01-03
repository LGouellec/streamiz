using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Operators
{
    internal class RootOperator : IOperator
    {
        public string Name => "ROOT-OPERATOR";

        public IList<IOperator> Previous { get; } = new List<IOperator>();

        public IList<IOperator> Next { get; } = new List<IOperator>();


        public void Init(ContextOperator context)
        {
            foreach (var n in Next)
                n.Init(context);
        }

        public void Kill()
        {
            foreach (var n in Next)
                n.Kill();
        }

        public void SetNextOperator(IOperator next)
        {
            if (!Next.Contains(next) && next != null)
                Next.Add(next);
        }

        public void SetPreviousOperator(IOperator prev)
        {
            if (!Previous.Contains(prev) && prev != null)
                Previous.Add(prev);
        }

        public void Start()
        {
            foreach (var n in Next)
                n.Start();
        }

        public void Stop()
        {
            foreach (var n in Next)
                n.Stop();
        }
    }
}
