using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors
{
    internal class RootProcessor : IProcessor
    {
        public string Name => "ROOT-OPERATOR";

        public IList<IProcessor> Previous { get; } = new List<IProcessor>();

        public IList<IProcessor> Next { get; } = new List<IProcessor>();


        public void Init(ProcessorContext context)
        {
            foreach (var n in Next)
                n.Init(context);
        }

        public void Kill()
        {
            foreach (var n in Next)
                n.Kill();
        }

        public void SetNextProcessor(IProcessor next)
        {
            if (!Next.Contains(next) && next != null)
                Next.Add(next);
        }

        public void SetPreviousProcessor(IProcessor prev)
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
