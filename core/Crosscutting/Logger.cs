using System;
using Microsoft.Extensions.Logging;
using ILogger = Microsoft.Extensions.Logging.ILogger;
using ILoggerFactory = Microsoft.Extensions.Logging.ILoggerFactory;

namespace Streamiz.Kafka.Net.Crosscutting
{
    /// <summary>
    /// Helper logger to get logger from type class
    /// </summary>
    public static class Logger
    {
        private static ILoggerFactory _factory = null;

        /// <summary>
        /// 
        /// </summary>
        public static ILoggerFactory LoggerFactory
        {
            get => _factory ??= Microsoft.Extensions.Logging.LoggerFactory.Create(builder => builder.AddConsole());
            set => _factory = value;
        }

        /// <summary>   
        /// Get logger from type class.
        /// By default, he search configuration file in root folder and filename 'log4net.config'.
        /// If configuration doesn't have appenders, a <see cref="ConsoleAppender"/> is added.
        /// If this file doesn't exist, please use : <see cref="Logger.GetLogger(Type, string)"/>
        /// </summary>
        /// <param name="type">Class type which call logger</param>
        /// <returns>Return logger configured</returns>
        public static ILogger GetLogger(Type type)
        {
            return LoggerFactory.CreateLogger(type);
        }
    }
}