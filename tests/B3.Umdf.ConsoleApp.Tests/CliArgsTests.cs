using B3.Umdf.Book;
using B3.Umdf.ConsoleApp;
using B3.Umdf.Server;

namespace B3.Umdf.ConsoleApp.Tests;

public class CliArgsTests
{
    [Fact]
    public void TryApply_NoArgs_LeavesSettingsUntouched_AndReturnsEmptyPositional()
    {
        var settings = new AppSettings { WsPort = 8080, Speed = 2.5 };
        var positional = new List<string>();

        var ok = CliArgs.TryApply(Array.Empty<string>(), settings, positional, out var error);

        Assert.True(ok);
        Assert.Null(error);
        Assert.Empty(positional);
        Assert.Equal(8080, settings.WsPort);
        Assert.Equal(2.5, settings.Speed);
    }

    [Fact]
    public void TryApply_KnownFlags_OverrideSettings()
    {
        var settings = new AppSettings { WsPort = null, Speed = 0, MulticastConfig = null, ReplayToMulticast = false };
        var positional = new List<string>();

        var ok = CliArgs.TryApply(
            new[] { "--ws-port", "9000", "--speed", "1.5", "--multicast-config", "cfg.json", "--replay-to-multicast" },
            settings, positional, out var error);

        Assert.True(ok);
        Assert.Null(error);
        Assert.Equal(9000, settings.WsPort);
        Assert.Equal(1.5, settings.Speed);
        Assert.Equal("cfg.json", settings.MulticastConfig);
        Assert.True(settings.ReplayToMulticast);
        Assert.Empty(positional);
    }

    [Fact]
    public void TryApply_PcapPrefixesFromCli_OverrideEnvJsonDefaults()
    {
        var settings = new AppSettings();
        settings.PcapPrefixes.Add("from-env-1");
        settings.PcapPrefixes.Add("from-env-2");

        var ok = CliArgs.TryApply(
            new[] { "--pcap-prefix", "cli-1", "--pcap-prefix", "cli-2" },
            settings, new List<string>(), out _);

        Assert.True(ok);
        Assert.Equal(new[] { "cli-1", "cli-2" }, settings.PcapPrefixes);
    }

    [Fact]
    public void TryApply_NoPcapPrefixOnCli_PreservesEnvJsonDefaults()
    {
        var settings = new AppSettings();
        settings.PcapPrefixes.Add("from-env");

        var ok = CliArgs.TryApply(Array.Empty<string>(), settings, new List<string>(), out _);

        Assert.True(ok);
        Assert.Equal(new[] { "from-env" }, settings.PcapPrefixes);
    }

    [Fact]
    public void TryApply_UnknownFlag_GoesToPositional()
    {
        var settings = new AppSettings();
        var positional = new List<string>();

        var ok = CliArgs.TryApply(
            new[] { "file1.pcap", "--ws-port", "8080", "file2.pcap" },
            settings, positional, out _);

        Assert.True(ok);
        Assert.Equal(8080, settings.WsPort);
        Assert.Equal(new[] { "file1.pcap", "file2.pcap" }, positional);
    }

    [Theory]
    [InlineData("0")]
    [InlineData("65536")]
    [InlineData("nope")]
    [InlineData("-1")]
    public void TryApply_InvalidWsPort_ReturnsError(string value)
    {
        var settings = new AppSettings();
        var ok = CliArgs.TryApply(new[] { "--ws-port", value }, settings, new List<string>(), out var error);

        Assert.False(ok);
        Assert.NotNull(error);
        Assert.Contains("--ws-port", error);
    }

    [Theory]
    [InlineData("-1")]
    [InlineData("not-a-number")]
    public void TryApply_InvalidSpeed_ReturnsError(string value)
    {
        var settings = new AppSettings();
        var ok = CliArgs.TryApply(new[] { "--speed", value }, settings, new List<string>(), out var error);

        Assert.False(ok);
        Assert.NotNull(error);
        Assert.Contains("--speed", error);
    }

    [Fact]
    public void TryApply_SpeedAcceptsDecimalWithInvariantCulture()
    {
        var settings = new AppSettings();
        var ok = CliArgs.TryApply(new[] { "--speed", "2.5" }, settings, new List<string>(), out _);

        Assert.True(ok);
        Assert.Equal(2.5, settings.Speed);
    }

    [Theory]
    [InlineData("channel", RecoveryMode.Channel)]
    [InlineData("Channel", RecoveryMode.Channel)]
    [InlineData("legacy", RecoveryMode.Channel)]
    [InlineData("per-symbol", RecoveryMode.PerSymbol)]
    [InlineData("PerSymbol", RecoveryMode.PerSymbol)]
    [InlineData("symbol", RecoveryMode.PerSymbol)]
    public void TryApply_RecoveryMode_AcceptsAliases(string value, RecoveryMode expected)
    {
        var settings = new AppSettings();
        var ok = CliArgs.TryApply(new[] { "--recovery-mode", value }, settings, new List<string>(), out _);

        Assert.True(ok);
        Assert.Equal(expected, settings.RecoveryMode);
    }

    [Fact]
    public void TryApply_RecoveryMode_InvalidReturnsError()
    {
        var settings = new AppSettings();
        var ok = CliArgs.TryApply(new[] { "--recovery-mode", "garbage" }, settings, new List<string>(), out var error);

        Assert.False(ok);
        Assert.NotNull(error);
        Assert.Contains("--recovery-mode", error);
    }

    [Fact]
    public void RecoveryMode_DefaultsToChannel()
    {
        Assert.Equal(RecoveryMode.Channel, new AppSettings().RecoveryMode);
    }
}
