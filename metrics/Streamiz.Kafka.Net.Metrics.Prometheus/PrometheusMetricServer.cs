using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Prometheus;

namespace Streamiz.Kafka.Net.Metrics.Prometheus
{
    public class PrometheusMetricServer : MetricServer
    {
        private readonly HttpListener _httpListener = new HttpListener();
        private static readonly object @lock = new object();
        private readonly IList<(Gauge, StreamMetric)> gauges = new List<(Gauge, StreamMetric)>();
        private CollectorRegistry collectorRegistry;

        public PrometheusMetricServer(int port, string url = "metrics/", CollectorRegistry registry = null, bool useHttps = false) 
            : this("+", port, url, registry, useHttps)
        { }

        private PrometheusMetricServer(string hostname, int port, string url = "metrics/", CollectorRegistry registry = null, bool useHttps = false) 
            : base(hostname, port, url, registry, useHttps)
        {
            var s = useHttps ? "s" : "";
            _httpListener.Prefixes.Add($"http{s}://{hostname}:{port}/{url}");
            _httpListener.Prefixes.Add($"http{s}://{hostname}:{port}/");
        }

        protected override Task StartServer(CancellationToken cancel)
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
                                    lock (@lock)
                                    {
                                        foreach (var metric in gauges)
                                        {
                                            Double value;
                                            if(Crosscutting.Utils.IsNumeric(metric.Item2.Value, out value))
                                                metric.Item1.WithLabels(metric.Item2.Tags.Values.ToArray()).Set(value);
                                            else
                                                metric.Item1.WithLabels(metric.Item2.Tags.Values.ToArray()).Set(1);
                                        }
                                    }

                                    using (MemoryStream ms = new MemoryStream())
                                    {
                                        if (collectorRegistry != null)
                                        {
                                            await collectorRegistry.CollectAndExportAsTextAsync(ms, cancel);

                                            ms.Position = 0;

                                            response.ContentType = PrometheusConstants.ExporterContentType;
                                            response.StatusCode = 200;
                                            await ms.CopyToAsync(response.OutputStream, 81920, cancel);
                                        }
                                    }

                                    response.OutputStream.Dispose();
                                }
                                catch (ScrapeFailedException ex)
                                {
                                    // This can only happen before anything is written to the stream, so it
                                    // should still be safe to update the status code and report an error.
                                    response.StatusCode = 503;

                                    if (!string.IsNullOrWhiteSpace(ex.Message))
                                    {
                                        using(var writer = new StreamWriter(response.OutputStream))
                                            writer.Write(ex.Message);
                                    }
                                }
                            }
                            catch (Exception ex) when (!(ex is OperationCanceledException))
                            {
                                if (!_httpListener.IsListening)
                                    return; // We were shut down.

                                Trace.WriteLine(string.Format("Error in {0}: {1}", nameof(MetricServer), ex));

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

        public void ClearGauges(CollectorRegistry collectorRegistry)
        {
            this.collectorRegistry = collectorRegistry;
            lock (@lock)
                gauges.Clear();
        }

        public void AddGauge(Gauge gauge, KeyValuePair<MetricName, StreamMetric> metric)
        {
            lock(@lock)
                gauges.Add((gauge, metric.Value));
        }
    }
}