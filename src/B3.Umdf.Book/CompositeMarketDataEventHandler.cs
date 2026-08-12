namespace B3.Umdf.Book;

public sealed class CompositeMarketDataEventHandler : IMarketDataEventHandler
{
    private readonly IMarketDataEventHandler[] _handlers;

    public CompositeMarketDataEventHandler(params IMarketDataEventHandler[] handlers)
    {
        _handlers = handlers;
    }

    public void OnSecurityStatusChanged(ulong securityId, InstrumentInfo info)
    {
        foreach (var h in _handlers) h.OnSecurityStatusChanged(securityId, info);
    }

    public void OnInstrumentStatusChanged(
        ulong securityId,
        InstrumentInfo info,
        in InstrumentStatusUpdate update)
    {
        foreach (var h in _handlers) h.OnInstrumentStatusChanged(securityId, info, in update);
    }

    public void OnMarketDataUpdated(ulong securityId, InstrumentInfo info)
    {
        foreach (var h in _handlers) h.OnMarketDataUpdated(securityId, info);
    }

    public void OnSecurityDefinitionChanged(ulong securityId, InstrumentInfo info)
    {
        foreach (var h in _handlers) h.OnSecurityDefinitionChanged(securityId, info);
    }

    public void OnPriceBandChanged(ulong securityId, InstrumentInfo info)
    {
        foreach (var h in _handlers) h.OnPriceBandChanged(securityId, info);
    }

    public void OnAuctionChanged(ulong securityId, InstrumentInfo info)
    {
        foreach (var h in _handlers) h.OnAuctionChanged(securityId, info);
    }

    public void OnInstrumentReplaced(ulong securityId, string? oldSymbol, string newSymbol)
    {
        foreach (var h in _handlers) h.OnInstrumentReplaced(securityId, oldSymbol, newSymbol);
    }

    public void OnNews(
        ulong securityIdOrZero,
        ulong newsId,
        byte source,
        ushort language,
        long origTimeNanos,
        ReadOnlySpan<byte> headline,
        ReadOnlySpan<byte> text,
        ReadOnlySpan<byte> url)
    {
        foreach (var h in _handlers) h.OnNews(securityIdOrZero, newsId, source, language, origTimeNanos, headline, text, url);
    }
}
