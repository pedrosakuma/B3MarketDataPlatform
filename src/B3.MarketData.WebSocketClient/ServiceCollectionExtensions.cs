using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace B3.MarketData.WebSocketClient;

/// <summary>
/// DI integration for <see cref="MarketDataClient"/>.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Register a singleton <see cref="MarketDataClient"/> configured by
    /// <paramref name="configureOptions"/>. The application is
    /// responsible for calling <c>ConnectAsync</c> at startup (e.g.
    /// from an <c>IHostedService</c>) and for disposing the client at
    /// shutdown — DI handles disposal automatically when the singleton
    /// scope is torn down.
    /// </summary>
    public static IServiceCollection AddMarketDataClient(
        this IServiceCollection services,
        Action<MarketDataClientOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configureOptions);

        services.AddOptions<MarketDataClientOptions>().Configure(configureOptions);
        services.TryAddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IOptions<MarketDataClientOptions>>().Value;
            var loggerFactory = sp.GetService<ILoggerFactory>();
            var logger = loggerFactory?.CreateLogger<MarketDataClient>();
            return new MarketDataClient(options, logger);
        });
        return services;
    }
}
