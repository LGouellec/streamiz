using log4net;
using System;
using System.IO;
using System.Reflection;
using System.Xml;

namespace kafka_stream_core.Crosscutting
{
    public static class Logger
    {
        private static bool configure = false;
        private static readonly string LOG_CONFIG_FILE = @"log4net.config";

        private static readonly log4net.ILog _log = GetLogger(typeof(Logger));

        public static ILog GetLogger(Type type) => GetLogger(type, LOG_CONFIG_FILE);

        public static ILog GetLogger(Type type, string configFile)
        {
            SetLog4NetConfiguration(configFile);
            return LogManager.GetLogger(type);
        }

        private static void SetLog4NetConfiguration(string configFile)
        {
            if (!configure)
            {
                XmlDocument log4netConfig = new XmlDocument();
                log4netConfig.Load(File.OpenRead(configFile));

                var repo = LogManager.CreateRepository(
                    Assembly.GetEntryAssembly(), typeof(log4net.Repository.Hierarchy.Hierarchy));

                log4net.Config.XmlConfigurator.Configure(repo, log4netConfig["log4net"]);
                configure = true;
            }
        }
    }
}
