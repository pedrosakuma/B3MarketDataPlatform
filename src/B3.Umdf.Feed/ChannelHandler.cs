using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Feed;

public sealed class ChannelHandler
{
    private readonly GapDetector _gapDetector = new();
    private readonly IFeedEventHandler _eventHandler;
    private bool _inRecovery;
    private long _packetsProcessed;
    private long _duplicatesSkipped;
    private long _gapsDetected;

    public bool InRecovery => _inRecovery;
    public uint ExpectedSequenceNumber => _gapDetector.ExpectedSequenceNumber;
    public long PacketsProcessed => Volatile.Read(ref _packetsProcessed);
    public long DuplicatesSkipped => Volatile.Read(ref _duplicatesSkipped);
    public long GapsDetected => Volatile.Read(ref _gapsDetected);

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
                _duplicatesSkipped++;
                return GapResult.Duplicate;

            case GapResult.Gap:
                _gapsDetected++;
                _inRecovery = true;
                _eventHandler.OnGapDetected(_gapDetector.ExpectedSequenceNumber - 1, header.SequenceNumber);
                break;
        }

        _packetsProcessed++;
        MessageDispatcher.Dispatch(in packet, span, _eventHandler);
        _eventHandler.OnPacketProcessed();
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
