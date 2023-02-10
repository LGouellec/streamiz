namespace Streamiz.Tests;

using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

public sealed class TestOutputLoggerProvider : ILoggerProvider
{
    private readonly ITestOutputHelper _output;

    public TestOutputLoggerProvider(ITestOutputHelper? output)
    {
        _output = output ?? throw new ArgumentNullException(nameof(output));
    }

    public ILogger CreateLogger(string categoryName)
        => new Logger(_output);

    public void Dispose() { }

    private class Logger : ILogger, IDisposable
    {
        public void Dispose() { }

        private readonly ITestOutputHelper? _output;

        public Logger(ITestOutputHelper? output)
            => _output = output;

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull
            => this;

        public bool IsEnabled(LogLevel logLevel)
            => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => _output?.WriteLine($"{logLevel} {eventId} {formatter(state, exception)}");
    }
}
