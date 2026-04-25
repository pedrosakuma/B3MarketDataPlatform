using B3.Umdf.Transport;

namespace B3.Umdf.Feed;

/// <summary>
/// Multiplexes feed events to multiple handlers.
/// Generic fan-out via IFeedEventHandler[]; specialized hot-path composites
/// (e.g. for the canonical book + market-data + symbol-registry triple) live in
/// the consuming layer (B3.Umdf.Book.OptimizedFeedComposite) to avoid pulling
/// concrete handler types into this lower layer.
/// </summary>
public sealed class CompositeFeedHandler : IFeedEventHandler
{
    private readonly IFeedEventHandler[] _handlers;

    public CompositeFeedHandler(params IFeedEventHandler[] handlers)
    {
        _handlers = handlers;
    }

    public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId)
    {
        foreach (var handler in _handlers)
            handler.OnPacket(in packet, sbePayload, templateId);
    }

    public void OnSequenceReset()
    {
        foreach (var handler in _handlers)
            handler.OnSequenceReset();
    }

    public void OnInstrumentDefinitionsComplete(int instrumentCount)
    {
        foreach (var handler in _handlers)
            handler.OnInstrumentDefinitionsComplete(instrumentCount);
    }

    public void OnPacketProcessed()
    {
        foreach (var handler in _handlers)
            handler.OnPacketProcessed();
    }

    public void OnSequenceVersionChanged(ushort newVersion)
    {
        foreach (var handler in _handlers)
            handler.OnSequenceVersionChanged(newVersion);
    }
}
