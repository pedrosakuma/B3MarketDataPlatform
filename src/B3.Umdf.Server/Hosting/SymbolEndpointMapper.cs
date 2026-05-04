using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace B3.Umdf.Server.Hosting;

internal sealed class SymbolEndpointMapper
{
    private readonly SubscriptionManager _subscriptionManager;

    public SymbolEndpointMapper(SubscriptionManager subscriptionManager)
    {
        _subscriptionManager = subscriptionManager;
    }

    public void Map(WebApplication app)
    {
        app.MapGet("/symbols", (string? q, int? limit) =>
        {
            var registry = _subscriptionManager.SymbolRegistry;
            if (registry is null)
                return Results.Json(new SymbolsResponse(), AppJsonContext.Default.SymbolsResponse);
            IEnumerable<string> symbols = registry.BySymbol.Keys.Order();
            if (!string.IsNullOrEmpty(q))
                symbols = symbols.Where(s => s.Contains(q, StringComparison.OrdinalIgnoreCase));
            var max = Math.Clamp(limit ?? 20, 1, 100);
            var list = symbols.Take(max).ToArray();
            return Results.Json(new SymbolsResponse { Count = registry.Count, Matched = list.Length, Symbols = list },
                AppJsonContext.Default.SymbolsResponse);
        });
    }
}
