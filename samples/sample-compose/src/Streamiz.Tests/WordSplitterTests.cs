namespace Streamiz.Tests;

using Microsoft.Extensions.Logging;
using Streamiz.Demo.Infrastructure;
using Streamiz.Demo.KStream;
using Streamiz.Demo.KStream.Models;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Xunit.Abstractions;

public class WordSplitterTests
{
    private readonly ITestOutputHelper output;

    public WordSplitterTests(ITestOutputHelper output)
    {
        this.output = output;
    }

    private void TestKStream(Action<StreamBuilder> configureTopology, Action<TopologyTestDriver> test)
    {
        StreamConfig config = new StreamConfig
        {
            ApplicationId = GetType().Name,
            SchemaRegistryUrl = "mock://test",
            AutoRegisterSchemas = true,
            Logger = LoggerFactory.Create(c => c.AddProvider(new TestOutputLoggerProvider(output)))
        };

        var builder = new StreamBuilder();
        configureTopology(builder);

        var driver = new TopologyTestDriver(builder.Build(), config);
        test(driver);
    }

    [Fact]
    public void TestTopol()
    {
        TestKStream(
            configureTopology: WordSplitter.ConfigureTopology,
            test: driver =>
            {
                var input = driver.CreateInputTopic<string, string, StringSerDes, StringSerDes>("input");
                var output = driver.CreateOuputTopic<string, DemoValue, StringSerDes, AvroSerDes<DemoValue>>("output");

                var message = "this is a test";
                var expectedWords = new[] { "THIS", "IS", "A", "TEST" };
                input.PipeInput(message);
                var values = output.ReadValueList().ToArray();
                values.Should().HaveCount(1);
                values[0].Input.Should().Be(message);
                values[0].ToUpperWords.Should().BeEquivalentTo(expectedWords);
            });
    }
}
