using B3.Umdf.ConsoleApp;
using Microsoft.Extensions.Logging;

namespace B3.Umdf.ConsoleApp.Tests;

public class LogLevelParserTests
{
    [Theory]
    [InlineData("Trace", LogLevel.Trace)]
    [InlineData("debug", LogLevel.Debug)]
    [InlineData("INFORMATION", LogLevel.Information)]
    [InlineData("Warning", LogLevel.Warning)]
    [InlineData("Error", LogLevel.Error)]
    [InlineData("Critical", LogLevel.Critical)]
    public void Parse_KnownValue_ReturnsMatchingLevel(string input, LogLevel expected)
    {
        Assert.Equal(expected, LogLevelParser.Parse(input));
    }

    [Theory]
    [InlineData("")]
    [InlineData("nope")]
    [InlineData("verbose")]
    public void Parse_UnknownValue_FallsBackToInformation(string input)
    {
        Assert.Equal(LogLevel.Information, LogLevelParser.Parse(input));
    }
}
