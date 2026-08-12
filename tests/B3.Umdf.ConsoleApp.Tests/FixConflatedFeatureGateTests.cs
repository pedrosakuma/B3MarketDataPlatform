using B3.Umdf.ConsoleApp;
using B3.Umdf.Server;

namespace B3.Umdf.ConsoleApp.Tests;

public sealed class FixConflatedFeatureGateTests
{
    [Fact]
    public void TryResolvePort_Disabled_ReturnsFalseWithoutError()
    {
        var settings = new AppSettings();

        bool enabled = FixConflatedFeatureGate.TryResolvePort(settings, out int port, out string? error);

        Assert.False(enabled);
        Assert.Equal(0, port);
        Assert.Null(error);
    }

    [Fact]
    public void TryResolvePort_EnabledWithoutPort_ReturnsValidationError()
    {
        var settings = new AppSettings { FixConflatedEnabled = true };

        bool enabled = FixConflatedFeatureGate.TryResolvePort(settings, out int port, out string? error);

        Assert.False(enabled);
        Assert.Equal(0, port);
        Assert.NotNull(error);
        Assert.Contains("UMDF_FIX_CONFLATED_PORT", error, StringComparison.Ordinal);
    }
}
