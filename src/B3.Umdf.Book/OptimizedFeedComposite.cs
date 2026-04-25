using B3.Umdf.Feed;
using B3.Umdf.Transport;

namespace B3.Umdf.Book;

/// <summary>
/// Specialized hot-path composite for the canonical
/// (BookManager, MarketDataManager, SymbolRegistry) fan-out.
///
/// Stores concrete sealed-typed references instead of the generic
/// IFeedEventHandler[]. The JIT can devirtualize and inline OnPacket calls on
/// sealed types — this eliminates ~3 virtual calls per SBE message on the
/// per-group hot path (≈10M vcalls/s at 815k pkt/s × 3 SBE/pkt × 3 handlers
/// in the e2e PCAP replay baseline).
/// </summary>
public sealed class OptimizedFeedComposite : IFeedEventHandler
{
    private readonly BookManager _book;
    private readonly MarketDataManager _md;
    private readonly SymbolRegistry _reg;

    public OptimizedFeedComposite(BookManager book, MarketDataManager md, SymbolRegistry reg)
    {
        _book = book;
        _md = md;
        _reg = reg;
    }

    public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId)
    {
        _book.OnPacket(in packet, sbePayload, templateId);
        _md.OnPacket(in packet, sbePayload, templateId);
        _reg.OnPacket(in packet, sbePayload, templateId);
    }

    public void OnSequenceReset()
    {
        _book.OnSequenceReset();
        _md.OnSequenceReset();
        _reg.OnSequenceReset();
    }

    public void OnInstrumentDefinitionsComplete(int instrumentCount)
    {
        _book.OnInstrumentDefinitionsComplete(instrumentCount);
        _md.OnInstrumentDefinitionsComplete(instrumentCount);
        _reg.OnInstrumentDefinitionsComplete(instrumentCount);
    }

    public void OnPacketProcessed()
    {
        _book.OnPacketProcessed();
        // MarketDataManager / SymbolRegistry use the default (no-op) interface
        // implementation; cast through the interface so the default impl is
        // dispatched. JIT can still devirtualize since the receiver types are sealed.
        ((IFeedEventHandler)_md).OnPacketProcessed();
        ((IFeedEventHandler)_reg).OnPacketProcessed();
    }

    public void OnSequenceVersionChanged(ushort newVersion)
    {
        _book.OnSequenceVersionChanged(newVersion);
        _md.OnSequenceVersionChanged(newVersion);
        ((IFeedEventHandler)_reg).OnSequenceVersionChanged(newVersion);
    }
}
