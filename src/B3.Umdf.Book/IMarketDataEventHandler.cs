namespace B3.Umdf.Book;

public interface IMarketDataEventHandler
{
    void OnSecurityStatusChanged(ulong securityId, InstrumentInfo info) { }
    void OnMarketDataUpdated(ulong securityId, InstrumentInfo info) { }
}
