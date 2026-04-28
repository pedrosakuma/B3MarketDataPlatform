namespace B3.Umdf.Book;

public interface IMarketDataEventHandler
{
    void OnSecurityStatusChanged(ulong securityId, InstrumentInfo info) { }
    void OnMarketDataUpdated(ulong securityId, InstrumentInfo info) { }

    /// <summary>
    /// Fired when a SecurityDefinition arrives for an existing SecurityID
    /// whose canonical identity (Symbol/ISIN/MaturityDate/SecurityType)
    /// differs from the cached value — i.e. the exchange is reusing the
    /// SecurityID for a different instrument (post-delisting recycle, or
    /// rare definition error). Receivers MUST treat this as a per-symbol
    /// epoch reset: clear book, drop stale buffers, reset registry baselines.
    /// </summary>
    void OnInstrumentReplaced(ulong securityId, string? oldSymbol, string newSymbol) { }
}
