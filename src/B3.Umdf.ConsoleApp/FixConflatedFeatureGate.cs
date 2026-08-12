using B3.Umdf.Server;

namespace B3.Umdf.ConsoleApp;

internal static class FixConflatedFeatureGate
{
    public static bool TryResolvePort(AppSettings settings, out int port, out string? error)
    {
        ArgumentNullException.ThrowIfNull(settings);

        if (!settings.FixConflatedEnabled)
        {
            port = 0;
            error = null;
            return false;
        }

        if (settings.FixConflatedPort is not int configuredPort || configuredPort is < 1 or > 65535)
        {
            port = 0;
            error = "UMDF_FIX_CONFLATED_ENABLED requires UMDF_FIX_CONFLATED_PORT to be set to a valid TCP port (1-65535).";
            return false;
        }

        port = configuredPort;
        error = null;
        return true;
    }
}
