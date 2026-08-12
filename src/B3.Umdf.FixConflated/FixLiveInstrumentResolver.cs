using B3.Umdf.Book;

namespace B3.Umdf.FixConflated;

public sealed class FixLiveInstrumentResolver : IFixMarketDataInstrumentResolver
{
    private const int DefaultPriceScale = 4;
    private readonly SymbolRegistry _symbolRegistry;
    private readonly IReadOnlyList<MarketDataManager> _marketDataManagers;

    public FixLiveInstrumentResolver(SymbolRegistry symbolRegistry, IReadOnlyList<MarketDataManager> marketDataManagers)
    {
        _symbolRegistry = symbolRegistry ?? throw new ArgumentNullException(nameof(symbolRegistry));
        _marketDataManagers = marketDataManagers ?? throw new ArgumentNullException(nameof(marketDataManagers));
    }

    public bool TryResolve(ulong securityId, out FixMarketDataInstrument instrument)
    {
        foreach (MarketDataManager manager in _marketDataManagers)
        {
            if (!manager.InstrumentData.TryGetValue(securityId, out InstrumentInfo? info) || string.IsNullOrWhiteSpace(info.Symbol))
                continue;

            instrument = new FixMarketDataInstrument(info.Symbol, securityId, ResolvePriceScale(info));
            return true;
        }

        if (_symbolRegistry.TryGetSymbol(securityId, out string symbol))
        {
            instrument = new FixMarketDataInstrument(symbol, securityId, DefaultPriceScale);
            return true;
        }

        instrument = default;
        return false;
    }

    internal static int ResolvePriceScale(InstrumentInfo info)
    {
        ArgumentNullException.ThrowIfNull(info);
        if (info.PriceDivisor is not long divisor || divisor <= 0)
            return DefaultPriceScale;

        int scale = 0;
        long remaining = divisor;
        while (remaining > 1 && remaining % 10 == 0)
        {
            remaining /= 10;
            scale++;
        }

        return remaining == 1 ? scale : DefaultPriceScale;
    }
}
