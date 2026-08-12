using B3.Umdf.Server;

namespace B3.Umdf.Server.Tests;

public sealed class AppSettingsFixConflatedEnvTests
{
    [Fact]
    public void ApplyEnvironment_ParsesFixConflatedKnobs()
    {
        WithEnv(new Dictionary<string, string?>
        {
            ["UMDF_FIX_CONFLATED_ENABLED"] = "true",
            ["UMDF_FIX_CONFLATED_PORT"] = "9501",
            ["UMDF_FIX_CONFLATED_CONFLATION_MS"] = "500",
            ["UMDF_FIX_CONFLATED_RESEND_BUFFER_CAPACITY"] = "2048",
            ["UMDF_FIX_CONFLATED_OUTBOUND_QUEUE_CAPACITY"] = "128",
            ["UMDF_FIX_CONFLATED_EVENT_QUEUE_CAPACITY"] = "4096",
        }, () =>
        {
            var settings = new AppSettings();
            settings.ApplyEnvironment();

            Assert.True(settings.FixConflatedEnabled);
            Assert.Equal(9501, settings.FixConflatedPort);
            Assert.Equal(500, settings.FixConflatedConflationWindowMs);
            Assert.Equal(2048, settings.FixConflatedResendBufferCapacity);
            Assert.Equal(128, settings.FixConflatedOutboundQueueCapacity);
            Assert.Equal(4096, settings.FixConflatedEventQueueCapacity);
        });
    }

    private static void WithEnv(Dictionary<string, string?> overrides, Action body)
    {
        var keys = new[]
        {
            "UMDF_FIX_CONFLATED_ENABLED",
            "UMDF_FIX_CONFLATED_PORT",
            "UMDF_FIX_CONFLATED_CONFLATION_MS",
            "UMDF_FIX_CONFLATED_RESEND_BUFFER_CAPACITY",
            "UMDF_FIX_CONFLATED_OUTBOUND_QUEUE_CAPACITY",
            "UMDF_FIX_CONFLATED_EVENT_QUEUE_CAPACITY",
        };
        var saved = new Dictionary<string, string?>(StringComparer.Ordinal);
        foreach (string key in keys)
        {
            saved[key] = Environment.GetEnvironmentVariable(key);
            Environment.SetEnvironmentVariable(key, null);
        }

        try
        {
            foreach (KeyValuePair<string, string?> entry in overrides)
                Environment.SetEnvironmentVariable(entry.Key, entry.Value);
            body();
        }
        finally
        {
            foreach (KeyValuePair<string, string?> entry in saved)
                Environment.SetEnvironmentVariable(entry.Key, entry.Value);
        }
    }
}
