using B3.Umdf.Server;

namespace B3.Umdf.Server.Tests;

public class AppSettingsStaleBufferTests
{
    [Fact]
    public void Defaults_AreSane()
    {
        var s = AppSettings.LoadDefault();
        Assert.Equal(512, s.StaleBufferGlobalMib);
        Assert.Equal(new[] { 8_192, 65_536, 262_144, 1_048_576 }, s.StaleBufferCapLevels);
    }

    [Fact]
    public void Env_OverridesGlobalMib()
    {
        Environment.SetEnvironmentVariable("UMDF_STALE_BUFFER_GLOBAL_MB", "1024");
        try
        {
            var s = AppSettings.LoadDefault();
            s.ApplyEnvironment();
            Assert.Equal(1024, s.StaleBufferGlobalMib);
        }
        finally { Environment.SetEnvironmentVariable("UMDF_STALE_BUFFER_GLOBAL_MB", null); }
    }

    [Fact]
    public void Env_OverridesCapLevels()
    {
        Environment.SetEnvironmentVariable("UMDF_STALE_BUFFER_CAP_LEVELS", "4096, 32768, 524288");
        try
        {
            var s = AppSettings.LoadDefault();
            s.ApplyEnvironment();
            Assert.Equal(new[] { 4_096, 32_768, 524_288 }, s.StaleBufferCapLevels);
        }
        finally { Environment.SetEnvironmentVariable("UMDF_STALE_BUFFER_CAP_LEVELS", null); }
    }

    [Theory]
    [InlineData("")]
    [InlineData("8192")]                  // single tier accepted (length 1)
    [InlineData("8192,4096")]             // not strictly increasing
    [InlineData("8192,8192")]             // not strictly increasing
    [InlineData("0,8192")]                // zero not allowed
    [InlineData("-1,8192")]               // negative not allowed
    [InlineData("foo,bar")]               // non-numeric
    public void Env_InvalidCapLevels_KeepsDefault(string raw)
    {
        Environment.SetEnvironmentVariable("UMDF_STALE_BUFFER_CAP_LEVELS", raw);
        try
        {
            var s = AppSettings.LoadDefault();
            s.ApplyEnvironment();
            // Single tier is the only valid case among these — verify it took effect.
            if (raw == "8192")
            {
                Assert.Equal(new[] { 8_192 }, s.StaleBufferCapLevels);
            }
            else
            {
                Assert.Equal(new[] { 8_192, 65_536, 262_144, 1_048_576 }, s.StaleBufferCapLevels);
            }
        }
        finally { Environment.SetEnvironmentVariable("UMDF_STALE_BUFFER_CAP_LEVELS", null); }
    }
}
