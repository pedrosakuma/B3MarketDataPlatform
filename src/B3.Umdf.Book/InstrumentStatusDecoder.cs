using B3.Umdf.Mbo.Sbe.V16;

namespace B3.Umdf.Book;

/// <summary>
/// Semantic halt/resume transition decoded from the exchange's
/// <c>SecurityStatus_3</c> template.
/// </summary>
public readonly record struct InstrumentStatusUpdate(
    int? PreviousStatus,
    int NewStatus,
    byte ReasonCode,
    ulong SourceTimestampNanos,
    uint? RptSeq);

/// <summary>
/// Decodes the proprietary halt/resume markers emitted by B3MatchingPlatform
/// in the existing UMDF <c>SecurityStatus_3</c> template.
/// </summary>
public static class InstrumentStatusDecoder
{
    public const byte InstrumentHaltedReasonCode = 1;
    public const byte InstrumentResumedReasonCode = 2;

    public static bool TryDecode(
        in SecurityStatus_3DataReader reader,
        int? previousStatus,
        out InstrumentStatusUpdate update)
    {
        ref readonly var message = ref reader.Data;
        byte reasonCode = message.SecurityTradingEvent is { } tradingEvent
            ? (byte)tradingEvent
            : byte.MaxValue;

        if (reasonCode is not (InstrumentHaltedReasonCode or InstrumentResumedReasonCode))
        {
            update = default;
            return false;
        }

        update = new InstrumentStatusUpdate(
            previousStatus,
            (int)message.SecurityTradingStatus,
            reasonCode,
            message.TransactTime.Time ?? 0,
            message.RptSeq);
        return true;
    }
}
