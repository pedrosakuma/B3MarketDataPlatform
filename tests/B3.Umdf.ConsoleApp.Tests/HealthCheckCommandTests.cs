using B3.Umdf.ConsoleApp;

namespace B3.Umdf.ConsoleApp.Tests;

public class HealthCheckCommandTests
{
    [Fact]
    public async Task TryRunAsync_NoArgs_ReturnsNull()
    {
        var result = await HealthCheckCommand.TryRunAsync(Array.Empty<string>());
        Assert.Null(result);
    }

    [Fact]
    public async Task TryRunAsync_NonHealthArg_ReturnsNull()
    {
        var result = await HealthCheckCommand.TryRunAsync(new[] { "--ws-port", "8080" });
        Assert.Null(result);
    }

    [Fact]
    public async Task TryRunAsync_HealthCheckArg_AttemptsConnection_ReturnsExitCode()
    {
        // No server is running on this port — the command must fail gracefully and
        // return 1 (error), never throw and never return null.
        Environment.SetEnvironmentVariable("UMDF_WS_PORT", "1"); // unprivileged-only port: connect refused
        try
        {
            var result = await HealthCheckCommand.TryRunAsync(new[] { "--health-check" });
            Assert.Equal(1, result);
        }
        finally
        {
            Environment.SetEnvironmentVariable("UMDF_WS_PORT", null);
        }
    }
}
