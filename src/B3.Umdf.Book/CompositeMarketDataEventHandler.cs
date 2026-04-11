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

    public void OnMarketDataUpdated(ulong securityId, InstrumentInfo info)
    {
        foreach (var h in _handlers) h.OnMarketDataUpdated(securityId, info);
    }
}
