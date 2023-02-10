namespace Streamiz.Demo.KStream.Models;
public class DemoValue
{
    public string? Input { get; init; }
    public string[]? ToUpperWords { get; init; }

    public static DemoValue FromString(string input) => new DemoValue
    {
        Input = input,
        ToUpperWords = input.ToUpperInvariant().Split(" ")
    };
}