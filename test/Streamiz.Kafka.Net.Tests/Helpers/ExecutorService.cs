using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Streamiz.Kafka.Net.Tests.Helpers
{
    internal class ExecutorService : IDisposable
    {
        private Action action;
        private readonly TimeSpan timeout;
        private Thread _thread;
        private bool disposed = false;

        private ExecutorService(Action action, TimeSpan timeout)
        {
            this.action = action;
            this.timeout = timeout;
            _thread = new Thread(() =>
            {
                DateTime dt = DateTime.Now;
                bool @continue = true;
                try
                {
                    while (@continue)
                    {
                        try
                        {
                            action();
                            @continue = false;
                        }
                        catch (AssertionException e)
                        {
                            if (dt.Add(timeout) < DateTime.Now)
                            {
                                @continue = false;
                                throw;
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    throw;
                }
            });
        }

        public static ExecutorService NewSingleThreadExecutor(Action action, TimeSpan timeout)
        {
            return new ExecutorService(action, timeout);
        }

        public void Start()
        {
            _thread.Start();
        }

        public void Stop()
        {
            _thread.Join();
            disposed = true;
        }

        public void Dispose()
        {
            if(!disposed)
                Stop();
        }
    }
}
