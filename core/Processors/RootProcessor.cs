using System;
using System.Collections.Generic;
using System.Text;
using kafka_stream_core.SerDes;

namespace kafka_stream_core.Processors
{
    internal class RootProcessor : IProcessor
    {
        public string Name => "ROOT-OPERATOR";

        public IList<IProcessor> Previous { get; } = new List<IProcessor>();

        public IList<IProcessor> Next { get; } = new List<IProcessor>();

        public ISerDes Key => null;

        public ISerDes Value => null;

        public IList<string> StateStores => new List<string>();

        public void Close()
        {
            
        }

        public void Init(ProcessorContext context)
        {
            foreach (var n in Next)
                n.Init(context);
        }

        public void Process(object key, object value)
        {
            foreach (var n in Next)
                n.Process(key, value);
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
    }
}
