using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Metrics.Prometheus
{
    public class PrometheusMetricServer : IDisposable
    {
        private readonly HttpListener _httpListener = new ();
        private static readonly object @lock = new();
        private readonly IList<(Gauge, StreamMetric)> gauges = new List<(Gauge, StreamMetric)>();
        private Task? task;

        public PrometheusMetricServer(int port, string url = "metrics/", bool useHttps = false) 
            : this("+", port, url, useHttps)
        { }

        private PrometheusMetricServer(string hostname, int port, string url = "metrics/", bool useHttps = false)
        {
            var s = useHttps ? "s" : "";
            _httpListener.Prefixes.Add($"http{s}://{hostname}:{port}/{url}");
            _httpListener.Prefixes.Add($"http{s}://{hostname}:{port}/");
        }

        public void Start(CancellationToken token)
        {
            if (task != null)
                throw new InvalidOperationException("The metric server has already been started.");
            
            task = StartServer(token);
        }

        public async Task StopAsync()
        {
            try
            {
                if (task == null)
                    return;
                await task.ConfigureAwait(false);
            }
            catch (OperationCanceledException ex)
            {
            }
        }

        public void Stop() => StopAsync().GetAwaiter().GetResult();

        void IDisposable.Dispose() => Stop();
        
        protected Task StartServer(CancellationToken cancel)
        {
            // This will ensure that any failures to start are nicely thrown from StartServerAsync.
            _httpListener.Start();

            // Kick off the actual processing to a new thread and return a Task for the processing thread.
            return Task.Factory.StartNew(() =>
            {
                try
                {
                    Thread.CurrentThread.Name = "Metric Server";     //Max length 16 chars (Linux limitation)
                    while (!cancel.IsCancellationRequested)
                    {
                        // There is no way to give a CancellationToken to GCA() so, we need to hack around it a bit.
                        var getContext = _httpListener.GetContextAsync();
                        getContext.Wait(cancel);
                        var context = getContext.Result;

                        // Kick the request off to a background thread for processing.
                        _ = Task.Factory.StartNew( async () => 
                        {
                            var request = context.Request;
                            var response = context.Response;

                            try
                            {
                                Thread.CurrentThread.Name = "Metric Process";

                                try
                                {
                                    List<Gauge> tmpGauges = null;
                                    
                                    lock (@lock)
                                    {
                                        foreach (var metric in gauges)
                                        {
                                            Double value;
                                            if(Utils.IsNumeric(metric.Item2.Value, out value))
                                                metric.Item1.SetValue(value);
                                            else
                                                metric.Item1.SetValue(1);
                                        }

                                        tmpGauges = gauges.Select(g => g.Item1).ToList();
                                    }

                                    using (var ms = new MemoryStream())
                                    {
                                        foreach (var metric in tmpGauges)
                                            ExportMetricAsTest(ms, metric);
                                        
                                        await ms.FlushAsync(cancel);

                                        ms.Seek(0, SeekOrigin.Begin);

                                        response.ContentType = "text/plain";
                                        response.StatusCode = 200;
                                        //var memoryBuffer = ms.GetBuffer();
                                        //response.OutputStream.Write(memoryBuffer, 0, memoryBuffer.Length);
                                        await ms.CopyToAsync(response.OutputStream, 2048*2*2, cancel);
                                    }

                                    response.OutputStream.Dispose();
                                }
                                catch (Exception ex)
                                {
                                    // This can only happen before anything is written to the stream, so it
                                    // should still be safe to update the status code and report an error.
                                    response.StatusCode = 503;

                                    if (!string.IsNullOrWhiteSpace(ex.Message))
                                    {
                                        using var writer = new StreamWriter(response.OutputStream);
                                        await writer.WriteAsync(ex.Message);
                                    }
                                }
                            }
                            catch (Exception ex) when (!(ex is OperationCanceledException))
                            {
                                if (!_httpListener.IsListening)
                                    return; // We were shut down.

                                Trace.WriteLine(string.Format("Error in {0}: {1}", nameof(PrometheusMetricServer), ex));

                                try
                                {
                                    response.StatusCode = 500;
                                }
                                catch
                                {
                                    // Might be too late in request processing to set response code, so just ignore.
                                }
                            }
                            finally
                            {
                                response.Close();
                            }
                        }, TaskCreationOptions.LongRunning);

                    }
                }
                finally
                {
                    _httpListener.Stop();
                    // This should prevent any currently processed requests from finishing.
                    _httpListener.Close();
                }
            }, TaskCreationOptions.LongRunning);
        }

        private void ExportMetricAsTest(MemoryStream ms, Gauge metric)
        {
            var tags = string.Join(",", metric.Labels.Select(kv => $"{kv.Key}=\"{kv.Value}\""));
            var formattedMetric =
                metric.Key + "{" + tags + "} " + metric.Value + Environment.NewLine;
            var buffer = Encoding.UTF8.GetBytes(formattedMetric);
            ms.Write(buffer, 0, buffer.Length);
        }

        internal void ClearGauges()
        {
            lock (@lock)
                gauges.Clear();
        }

        internal void AddGauge(Gauge gauge, KeyValuePair<MetricName, StreamMetric> metric)
        {
            lock(@lock)
                gauges.Add((gauge, metric.Value));
        }
    }
}