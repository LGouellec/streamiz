using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Core;
using log4net.Layout;
using log4net.Repository.Hierarchy;
using System;
using System.IO;
using System.Reflection;
using System.Xml;

namespace Streamiz.Kafka.Net.Crosscutting
{
    public static class Logger
    {
        private static Level defaultLevel = Level.Info;
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
                Hierarchy hierarchy = (Hierarchy)logger.Logger.Repository;

                PatternLayout patternLayout = new PatternLayout();
                patternLayout.ConversionPattern = PatternLayout.DetailConversionPattern;
                patternLayout.ActivateOptions();

                ConsoleAppender console = new ConsoleAppender();
                console.Layout = patternLayout;
                console.Threshold = defaultLevel;
                hierarchy.Root.AddAppender(console);

                hierarchy.Root.Level = defaultLevel;

                BasicConfigurator.Configure(hierarchy, console);
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

                    var repo = LogManager.CreateRepository("setup");

                    log4net.Config.XmlConfigurator.Configure(repo, log4netConfig["log4net"]);
                    configure = true;
                }
            }
        }
    }
}
