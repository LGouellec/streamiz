# Test topology driver

Must be used for testing your stream topology.
Usage: 
``` csharp
static void Main(string[] args)
{
    var config = new StreamConfig<StringSerDes, StringSerDes>();
    config.ApplicationId = "test-test-driver-app";
    
    StreamBuilder builder = new StreamBuilder();

    builder.Stream<string, string>("test")
        .Filter((k, v) => v.Contains("test"))
        .To("test-output");

    Topology t = builder.Build();

    using (var driver = new TopologyTestDriver(t, config))
    {
        var inputTopic = driver.CreateInputTopic<string, string>("test");
        var outputTopic = driver.CreateOuputTopic<string, string>("test-output", TimeSpan.FromSeconds(5));
        inputTopic.PipeInput("test", "test-1234");
        var r = outputTopic.ReadKeyValue();
    }
}
```