using B3.Umdf.Server.Hosting;
using Xunit;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Defaults snapshot for the <see cref="ClientSessionOptions"/> record that
/// groups the per-WebSocket-connection tuning surface. The defaults must
/// match the WebSocketHost ctor defaults so existing call sites can
/// continue to omit the parameters and inherit the same behavior.
/// </summary>
public class ClientSessionOptionsTests
{
    [Fact]
    public void Defaults_match_WebSocketHost_ctor_defaults()
    {
        var opts = new ClientSessionOptions();
        Assert.Equal(4096, opts.ChannelCapacity);
        Assert.Equal(0.75, opts.SlowClientThreshold);
        Assert.Equal(100, opts.SlowClientMaxTicks);
        Assert.Equal(16L * 1024 * 1024, opts.MaxPendingBytes);
        Assert.Equal(0, opts.CoalesceWindowMs);
    }

    [Fact]
    public void Record_with_expression_supports_partial_overrides()
    {
        var opts = new ClientSessionOptions() with { CoalesceWindowMs = 10, MaxPendingBytes = 32L * 1024 * 1024 };
        Assert.Equal(10, opts.CoalesceWindowMs);
        Assert.Equal(32L * 1024 * 1024, opts.MaxPendingBytes);
        Assert.Equal(4096, opts.ChannelCapacity);
    }
}
