using B3.Umdf.ConsoleApp;
using B3.Umdf.PcapReplay;
using B3.Umdf.Transport;

namespace B3.Umdf.ConsoleApp.Tests;

public class ReplaySourcesBuilderTests : IDisposable
{
    private readonly string _tempDir;

    public ReplaySourcesBuilderTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "umdf-replay-builder-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { /* ignore */ }
    }

    [Fact]
    public void TryBuild_PcapPrefix_DiscoversFourChannelsByConvention()
    {
        var prefix = Path.Combine(_tempDir, "EQT");
        Touch($"{prefix}_Incremental_FeedA.pcap");
        Touch($"{prefix}_Incremental_FeedB.pcap");
        Touch($"{prefix}_InstrumentDefinition.pcap");
        Touch($"{prefix}_SnapshotRecovery.pcap");

        var ok = ReplaySourcesBuilder.TryBuild(
            new[] { prefix }, Array.Empty<string>(),
            out var sources, out var groupIds);

        Assert.True(ok);
        Assert.Equal(new[] { 0 }, groupIds);
        Assert.Equal(4, sources.Count);
        Assert.Contains(sources, s => s.Channel == ChannelType.IncrementalA);
        Assert.Contains(sources, s => s.Channel == ChannelType.IncrementalB);
        Assert.Contains(sources, s => s.Channel == ChannelType.InstrumentDefinition);
        Assert.Contains(sources, s => s.Channel == ChannelType.SnapshotRecovery);
        Assert.All(sources, s => Assert.Equal(0, s.Group));
    }

    [Fact]
    public void TryBuild_MultiplePcapPrefixes_CreatesOneGroupPerPrefix()
    {
        var p1 = Path.Combine(_tempDir, "EQT");
        var p2 = Path.Combine(_tempDir, "DRV");
        foreach (var p in new[] { p1, p2 })
        {
            Touch($"{p}_Incremental_FeedA.pcap");
            Touch($"{p}_Incremental_FeedB.pcap");
            Touch($"{p}_InstrumentDefinition.pcap");
            Touch($"{p}_SnapshotRecovery.pcap");
        }

        var ok = ReplaySourcesBuilder.TryBuild(
            new[] { p1, p2 }, Array.Empty<string>(),
            out var sources, out var groupIds);

        Assert.True(ok);
        Assert.Equal(new[] { 0, 1 }, groupIds);
        Assert.Equal(8, sources.Count);
        Assert.Equal(4, sources.Count(s => s.Group == 0));
        Assert.Equal(4, sources.Count(s => s.Group == 1));
    }

    [Fact]
    public void TryBuild_MissingPrefixedFile_ReturnsFalse()
    {
        var prefix = Path.Combine(_tempDir, "EQT");
        Touch($"{prefix}_Incremental_FeedA.pcap");
        // Other 3 missing.

        var ok = ReplaySourcesBuilder.TryBuild(
            new[] { prefix }, Array.Empty<string>(), out _, out _);

        Assert.False(ok);
    }

    [Fact]
    public void TryBuild_PositionalArgs_AssignsChannelsInOrder_AllToGroupZero()
    {
        var f1 = Touch(Path.Combine(_tempDir, "a.pcap"));
        var f2 = Touch(Path.Combine(_tempDir, "b.pcap"));
        var f3 = Touch(Path.Combine(_tempDir, "i.pcap"));

        var ok = ReplaySourcesBuilder.TryBuild(
            Array.Empty<string>(), new[] { f1, f2, f3 },
            out var sources, out var groupIds);

        Assert.True(ok);
        Assert.Equal(new[] { 0 }, groupIds);
        Assert.Equal(3, sources.Count);
        Assert.Equal(ChannelType.IncrementalA, sources[0].Channel);
        Assert.Equal(ChannelType.IncrementalB, sources[1].Channel);
        Assert.Equal(ChannelType.InstrumentDefinition, sources[2].Channel);
    }

    [Fact]
    public void TryBuild_NoInputs_PrintsUsage_AndReturnsFalse()
    {
        var ok = ReplaySourcesBuilder.TryBuild(
            Array.Empty<string>(), Array.Empty<string>(),
            out var sources, out var groupIds);

        Assert.False(ok);
        Assert.Empty(sources);
        Assert.Empty(groupIds);
    }

    private static string Touch(string path)
    {
        File.WriteAllBytes(path, Array.Empty<byte>());
        return path;
    }
}
