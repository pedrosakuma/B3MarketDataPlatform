using B3.Umdf.Mbo.Sbe.V17;

namespace B3.Umdf.Book;

/// <summary>
/// Administrative instrument state decoded from the exchange's legacy
/// <c>SecurityStatus_3</c> or authoritative <c>InstrumentStatus_58</c> template.
/// </summary>
public readonly record struct InstrumentStatusUpdate(
    int? PreviousStatus,
    int NewStatus,
    byte TransitionCode,
    byte? HaltReasonCode,
    ulong SourceTimestampNanos,
    uint? RptSeq,
    byte? AdministrativeStateCode = null,
    byte? TradingSessionId = null,
    bool IsRecovery = false)
{
    public bool IsHalted => AdministrativeStateCode switch
    {
        InstrumentStatusDecoder.AdministrativeHaltedStateCode => true,
        InstrumentStatusDecoder.AdministrativeActiveStateCode => false,
        _ => TransitionCode == InstrumentStatusDecoder.InstrumentHaltedTransitionCode,
    };
}

/// <summary>
/// Decodes B3MatchingPlatform administrative halt state. Template 58 is the
/// authoritative V17 contract; template 3 remains supported during rollout.
/// </summary>
public static class InstrumentStatusDecoder
{
    public const byte UnavailableCode = byte.MaxValue;
    public const byte InstrumentHaltedTransitionCode = 1;
    public const byte InstrumentResumedTransitionCode = 2;
    public const byte AdministrativeActiveStateCode = 0;
    public const byte AdministrativeHaltedStateCode = 1;

    public static bool TryDecode(
        in SecurityStatus_3DataReader reader,
        int? previousStatus,
        out InstrumentStatusUpdate update)
    {
        ref readonly var message = ref reader.Data;
        byte transitionCode = message.SecurityTradingEvent is { } tradingEvent
            ? (byte)tradingEvent
            : byte.MaxValue;

        if (transitionCode is not (InstrumentHaltedTransitionCode or InstrumentResumedTransitionCode))
        {
            update = default;
            return false;
        }

        update = new InstrumentStatusUpdate(
            previousStatus,
            (int)message.SecurityTradingStatus,
            transitionCode,
            null,
            message.TransactTime.Time ?? 0,
            message.RptSeq,
            transitionCode == InstrumentHaltedTransitionCode
                ? AdministrativeHaltedStateCode
                : AdministrativeActiveStateCode);
        return true;
    }

    public static bool TryDecode(
        in InstrumentStatus_58DataReader reader,
        int? previousStatus,
        out InstrumentStatusUpdate update)
    {
        ref readonly var message = ref reader.Data;
        byte stateCode = (byte)message.AdministrativeHaltState;
        byte transitionCode = message.AdministrativeTransitionKind is { } transition
            ? (byte)transition
            : UnavailableCode;
        byte? haltReasonCode = message.HaltReason is { } haltReason
            ? (byte)haltReason
            : null;
        bool isRecovery = message.MatchEventIndicator.IsRecoveryMsg();

        if (stateCode == AdministrativeHaltedStateCode && haltReasonCode is null
            || stateCode == AdministrativeActiveStateCode && haltReasonCode is not null
            || isRecovery && transitionCode != UnavailableCode
            || !isRecovery && transitionCode == UnavailableCode)
        {
            update = default;
            return false;
        }

        update = new InstrumentStatusUpdate(
            previousStatus,
            (int)message.SecurityTradingStatus,
            transitionCode,
            haltReasonCode,
            message.TransactTime.Time ?? 0,
            message.RptSeq,
            stateCode,
            (byte)message.TradingSessionID,
            isRecovery);
        return true;
    }
}
