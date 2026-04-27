using B3.Umdf.Server;

namespace B3.Umdf.Server.Tests;

/// <summary>
/// Pins the legacy environment variable contract folded into
/// <see cref="AppSettings.ApplyEnvironment"/>: PCAP_DIR, PCAP_PREFIX,
/// WS_PORT, and REPLAY_SPEED are honored as fallbacks for the shell-less
/// Docker compose images, but the newer UMDF_* names always take
/// precedence when both are present.
/// </summary>
public class AppSettingsLegacyEnvTests
{
    [Fact]
    public void PcapPrefix_Env_PopulatesPrefixes_PrependedWithPcapDir()
    {
        WithEnv(new()
        {
            ["PCAP_DIR"] = "/tmp/pcaps",
            ["PCAP_PREFIX"] = "alpha,beta,gamma",
        }, () =>
        {
            var s = new AppSettings();
            s.ApplyEnvironment();
            Assert.Equal(new[]
            {
                "/tmp/pcaps/alpha",
                "/tmp/pcaps/beta",
                "/tmp/pcaps/gamma",
            }, s.PcapPrefixes);
            Assert.Equal("/tmp/pcaps", s.PcapDirectory);
        });
    }

    [Fact]
    public void PcapPrefix_Env_DoesNotOverwriteExistingPrefixes()
    {
        WithEnv(new()
        {
            ["PCAP_DIR"] = "/tmp/pcaps",
            ["PCAP_PREFIX"] = "alpha",
        }, () =>
        {
            var s = new AppSettings();
            s.PcapPrefixes.Add("/explicit/path/foo");
            s.ApplyEnvironment();
            // JSON / CLI-populated prefixes win over the legacy env fallback.
            Assert.Single(s.PcapPrefixes);
            Assert.Equal("/explicit/path/foo", s.PcapPrefixes[0]);
        });
    }

    [Fact]
    public void WsPort_LegacyEnv_AppliesOnlyWhenUmdfNotSet()
    {
        WithEnv(new()
        {
            ["UMDF_WS_PORT"] = "9000",
            ["WS_PORT"] = "8000",
        }, () =>
        {
            var s = new AppSettings();
            s.ApplyEnvironment();
            // UMDF_WS_PORT wins.
            Assert.Equal(9000, s.WsPort);
        });

        WithEnv(new()
        {
            ["WS_PORT"] = "8000",
        }, () =>
        {
            var s = new AppSettings();
            s.ApplyEnvironment();
            Assert.Equal(8000, s.WsPort);
        });
    }

    [Fact]
    public void Speed_LegacyEnv_AppliesOnlyWhenUmdfNotSet()
    {
        WithEnv(new()
        {
            ["UMDF_SPEED"] = "5",
            ["REPLAY_SPEED"] = "10",
        }, () =>
        {
            var s = new AppSettings();
            s.ApplyEnvironment();
            // UMDF_SPEED wins.
            Assert.Equal(5.0, s.Speed);
        });

        WithEnv(new()
        {
            ["REPLAY_SPEED"] = "2.5",
        }, () =>
        {
            var s = new AppSettings();
            s.ApplyEnvironment();
            Assert.Equal(2.5, s.Speed);
        });
    }

    [Fact]
    public void PcapDirectory_DefaultsTo_AppPcap_WhenEnvNotSet()
    {
        WithEnv(new(), () =>
        {
            var s = new AppSettings();
            s.ApplyEnvironment();
            Assert.Equal("/app/pcap", s.PcapDirectory);
        });
    }

    private static void WithEnv(Dictionary<string, string?> overrides, Action body)
    {
        // Snapshot every key we touch (including UMDF_* counterparts) so the test
        // is hermetic regardless of host environment.
        var keys = new HashSet<string>(overrides.Keys, StringComparer.Ordinal)
        {
            "PCAP_DIR", "PCAP_PREFIX",
            "WS_PORT", "UMDF_WS_PORT",
            "REPLAY_SPEED", "UMDF_SPEED",
        };
        var saved = new Dictionary<string, string?>(StringComparer.Ordinal);
        foreach (var k in keys)
        {
            saved[k] = Environment.GetEnvironmentVariable(k);
            Environment.SetEnvironmentVariable(k, null);
        }
        try
        {
            foreach (var kv in overrides)
                Environment.SetEnvironmentVariable(kv.Key, kv.Value);
            body();
        }
        finally
        {
            foreach (var kv in saved)
                Environment.SetEnvironmentVariable(kv.Key, kv.Value);
        }
    }
}
