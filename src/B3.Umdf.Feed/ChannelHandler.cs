using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed;

public sealed class ChannelHandler
{
    private readonly GapDetector _gapDetector = new();
    private readonly IFeedEventHandler _eventHandler;
    private bool _inRecovery;

    public bool InRecovery => _inRecovery;
    public uint ExpectedSequenceNumber => _gapDetector.ExpectedSequenceNumber;

    public ChannelHandler(IFeedEventHandler eventHandler)
    {
        _eventHandler = eventHandler;
    }

    public GapResult HandlePacket(in UmdfPacket packet)
    {
        var span = packet.Data.Span;
        if (!PacketHeader.TryParse(span, out var header, out _))
            return GapResult.InSequence;

        var gapResult = _gapDetector.Check(header.SequenceNumber);

        switch (gapResult)
        {
            case GapResult.Duplicate:
                return GapResult.Duplicate;

            case GapResult.Gap:
                _inRecovery = true;
                _eventHandler.OnGapDetected(_gapDetector.ExpectedSequenceNumber - 1, header.SequenceNumber);
                break;
        }

        MessageDispatcher.Dispatch(in packet, _eventHandler);
        return gapResult;
    }

    public void Reset()
    {
        _gapDetector.Reset();
        _inRecovery = false;
    }

    public void CompleteRecovery(uint nextSeqNum)
    {
        _gapDetector.Reset(nextSeqNum);
        _inRecovery = false;
    }
}
