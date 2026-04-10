using B3.Umdf.Feed;

namespace B3.Umdf.Feed.Tests;

public class GapDetectorTests
{
    [Fact]
    public void InSequence_ReturnsInSequence()
    {
        var detector = new GapDetector();

        Assert.Equal(GapResult.InSequence, detector.Check(1));
        Assert.Equal(GapResult.InSequence, detector.Check(2));
        Assert.Equal(GapResult.InSequence, detector.Check(3));
        Assert.Equal(4u, detector.ExpectedSequenceNumber);
    }

    [Fact]
    public void Duplicate_ReturnsDuplicate()
    {
        var detector = new GapDetector();

        detector.Check(1);
        detector.Check(2);
        Assert.Equal(GapResult.Duplicate, detector.Check(1));
        Assert.Equal(GapResult.Duplicate, detector.Check(2));
    }

    [Fact]
    public void Gap_ReturnsGap()
    {
        var detector = new GapDetector();

        detector.Check(1);
        Assert.Equal(GapResult.Gap, detector.Check(5));
        Assert.Equal(6u, detector.ExpectedSequenceNumber);
    }

    [Fact]
    public void Reset_ResetsExpectedSequence()
    {
        var detector = new GapDetector();

        detector.Check(1);
        detector.Check(2);
        detector.Reset(10);

        Assert.Equal(10u, detector.ExpectedSequenceNumber);
        Assert.Equal(GapResult.InSequence, detector.Check(10));
    }
}
