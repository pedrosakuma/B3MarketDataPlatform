using Microsoft.AspNetCore.Builder;

namespace B3.Umdf.Server.Hosting;

/// <summary>
/// Permissive CORS middleware shared across the read-only public endpoints
/// (dashboard scrape + health probes). Tighten via reverse proxy if exposed
/// externally.
/// </summary>
internal static class CorsMiddleware
{
    public static void UsePermissiveCors(this WebApplication app)
    {
        app.Use(async (context, next) =>
        {
            context.Response.Headers["Access-Control-Allow-Origin"] = "*";
            context.Response.Headers["Access-Control-Allow-Methods"] = "GET, OPTIONS";
            context.Response.Headers["Access-Control-Allow-Headers"] = "Content-Type";
            if (context.Request.Method == "OPTIONS")
            {
                context.Response.StatusCode = 204;
                return;
            }
            await next();
        });
    }
}
