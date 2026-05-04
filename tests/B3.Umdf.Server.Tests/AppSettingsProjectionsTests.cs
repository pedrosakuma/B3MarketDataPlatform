using B3.Umdf.Server.Configuration;
using Xunit;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Verifies the read-only option-record projections layered on top of
/// <see cref="AppSettings"/>: each helper must surface the same defaults
/// the flat property bag exposes, and must reflect overrides applied to
/// the source <see cref="AppSettings"/> instance.
/// </summary>
public class AppSettingsProjectionsTests
{
    [Fact]
    public void GetConnectionLimits_mirrors_defaults_and_overrides()
    {
        var s = new AppSettings();
        var defaults = s.GetConnectionLimits();
        Assert.Equal(s.MaxConnections, defaults.MaxConnections);
        Assert.Equal(s.ClientChannelCapacity, defaults.ClientChannelCapacity);
        Assert.Equal(s.SlowClientThreshold, defaults.SlowClientThreshold);
        Assert.Equal(s.ClientMaxPendingBytes, defaults.ClientMaxPendingBytes);
        Assert.Equal(s.ClientCoalesceWindowMs, defaults.ClientCoalesceWindowMs);

        s.MaxConnections = 1234;
        s.ClientCoalesceWindowMs = 25;
        var updated = s.GetConnectionLimits();
        Assert.Equal(1234, updated.MaxConnections);
        Assert.Equal(25, updated.ClientCoalesceWindowMs);
    }

    [Fact]
    public void GetRecoveryHealthOptions_carries_health_gate_and_drain_budget()
    {
        var s = new AppSettings
        {
            HealthMaxStaleSeconds = 90,
            HealthFailOnRecovery = false,
            ShutdownDrainSeconds = 12,
        };
        var opts = s.GetRecoveryHealthOptions();
        Assert.Equal(90, opts.HealthMaxStaleSeconds);
        Assert.False(opts.HealthFailOnRecovery);
        Assert.Equal(12, opts.ShutdownDrainSeconds);
    }

    [Fact]
    public void GetBufferingOptions_carries_stale_buffer_ladder_and_window()
    {
        var s = new AppSettings();
        var opts = s.GetBufferingOptions();
        Assert.Equal(s.StaleBufferGlobalMib, opts.StaleBufferGlobalMib);
        Assert.Equal(s.StaleBufferCapLevels, opts.StaleBufferCapLevels);
        Assert.Equal(s.ServerFlushWindowMs, opts.ServerFlushWindowMs);
        Assert.Equal(s.PerSymbolFanoutSuppressHighPct, opts.PerSymbolFanoutSuppressHighPct);
    }

    [Fact]
    public void GetReplayOptions_exposes_loss_profile_and_pcap_inputs()
    {
        var s = new AppSettings
        {
            Speed = 2.5,
            ReplayToMulticast = true,
            LossTargets = "AB",
            LossRate = 0.01,
            LossMode = "burst",
            LossBurstSize = 5,
            LossCorrelated = true,
            LossSeed = 42,
        };
        s.PcapPrefixes.Add("/data/foo");
        var opts = s.GetReplayOptions();
        Assert.Equal(2.5, opts.Speed);
        Assert.True(opts.ReplayToMulticast);
        Assert.Equal("AB", opts.LossTargets);
        Assert.Equal("burst", opts.LossMode);
        Assert.Equal(5, opts.LossBurstSize);
        Assert.True(opts.LossCorrelated);
        Assert.Equal(42, opts.LossSeed);
        Assert.Single(opts.PcapPrefixes);
        Assert.Equal("/data/foo", opts.PcapPrefixes[0]);
    }
}
