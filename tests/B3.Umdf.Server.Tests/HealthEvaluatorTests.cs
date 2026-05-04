using B3.Umdf.Server.Hosting;
using Xunit;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Pure-logic unit tests for the staleness gate behind <c>GET /health</c>.
/// The full HTTP-level contract is exercised by <see cref="HealthEndpointTests"/>;
/// these tests cover the rule matrix without spinning up Kestrel so future
/// edge cases (cold start, never-seen group, gate disabled) can be added
/// cheaply.
/// </summary>
public class HealthEvaluatorTests
{
    [Fact]
    public void Returns_null_when_failOnRecovery_disabled()
    {
        var states = new Dictionary<string, string> { ["g1"] = "WaitInstrumentDefinition" };
        var result = HealthEvaluator.FindUnhealthyGroups(
            states, lastPacketTicks: null,
            nowTicks: 1_000_000, uptimeSeconds: 600,
            maxStaleSeconds: 60, failOnRecovery: false);
        Assert.Null(result);
    }

    [Fact]
    public void Returns_null_when_no_states_provided()
    {
        var result = HealthEvaluator.FindUnhealthyGroups(
            states: null, lastPacketTicks: null,
            nowTicks: 1_000_000, uptimeSeconds: 600,
            maxStaleSeconds: 60, failOnRecovery: true);
        Assert.Null(result);
    }

    [Fact]
    public void Streaming_groups_are_never_unhealthy()
    {
        var states = new Dictionary<string, string>
        {
            ["g1"] = "Streaming",
            ["g2"] = "Streaming",
        };
        var lastPackets = new Dictionary<string, long> { ["g1"] = 0, ["g2"] = 0 };
        var result = HealthEvaluator.FindUnhealthyGroups(
            states, lastPackets,
            nowTicks: 1_000_000, uptimeSeconds: 99_999,
            maxStaleSeconds: 60, failOnRecovery: true);
        Assert.Null(result);
    }

    [Fact]
    public void Cold_start_uses_uptime_below_threshold_and_stays_healthy()
    {
        var states = new Dictionary<string, string> { ["g1"] = "WaitInstrumentDefinition" };
        var result = HealthEvaluator.FindUnhealthyGroups(
            states, lastPacketTicks: null,
            nowTicks: 1_000_000, uptimeSeconds: 30,
            maxStaleSeconds: 60, failOnRecovery: true);
        Assert.Null(result);
    }

    [Fact]
    public void Non_streaming_group_past_threshold_via_lastPacket_is_unhealthy()
    {
        var states = new Dictionary<string, string> { ["g1"] = "WaitInstrumentDefinition" };
        // Last packet 90 seconds ago (90_000 ms in TickCount64 units).
        var lastPackets = new Dictionary<string, long> { ["g1"] = 1_000_000 - 90_000 };
        var result = HealthEvaluator.FindUnhealthyGroups(
            states, lastPackets,
            nowTicks: 1_000_000, uptimeSeconds: 9999,
            maxStaleSeconds: 60, failOnRecovery: true);
        Assert.NotNull(result);
        Assert.Single(result!);
        Assert.Contains("g1=WaitInstrumentDefinition", result![0]);
    }

    [Fact]
    public void Negative_threshold_disables_gate()
    {
        var states = new Dictionary<string, string> { ["g1"] = "WaitInstrumentDefinition" };
        var result = HealthEvaluator.FindUnhealthyGroups(
            states, lastPacketTicks: null,
            nowTicks: 1_000_000, uptimeSeconds: 99_999,
            maxStaleSeconds: -1, failOnRecovery: true);
        Assert.Null(result);
    }
}
