namespace B3.Umdf.Feed;

public sealed class GapDetector
{
    private uint _expectedSeqNum = 1;

    public uint ExpectedSequenceNumber => _expectedSeqNum;

    public GapResult Check(uint sequenceNumber)
    {
        if (sequenceNumber == _expectedSeqNum)
        {
            _expectedSeqNum++;
            return GapResult.InSequence;
        }
        if (sequenceNumber < _expectedSeqNum)
            return GapResult.Duplicate;

        // Gap detected
        var result = GapResult.Gap;
        _expectedSeqNum = sequenceNumber + 1;
        return result;
    }

    public void Reset(uint nextExpected = 1)
    {
        _expectedSeqNum = nextExpected;
    }
}

public enum GapResult
{
    InSequence,
    Duplicate,
    Gap
}
