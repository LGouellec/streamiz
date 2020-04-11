using log4net;
using log4net.Appender;
using log4net.Core;
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

            var logger = LogManager.GetLogger(type);
            if (logger.Logger is log4net.Repository.Hierarchy.Logger &&
                ((log4net.Repository.Hierarchy.Logger)logger.Logger).Appenders.Count == 0 &&
                ((log4net.Repository.Hierarchy.Logger)logger.Logger).Parent.Appenders.Count == 0)
            {
                ((log4net.Repository.Hierarchy.Logger)logger.Logger).AddAppender(new ConsoleAppender() { Threshold = Level.Info });
                ((log4net.Repository.Hierarchy.Logger)logger.Logger).Level = Level.Info;
            }
            return logger;
        }

        private static void SetLog4NetConfiguration(string configFile)
        {
            if (!configure)
            {
                if (File.Exists(configFile))
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
}
