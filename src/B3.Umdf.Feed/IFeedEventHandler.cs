using B3.Umdf.Transport;

namespace B3.Umdf.Feed;

public interface IFeedEventHandler
{
    void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId);

    /// <summary>
    /// Channel-level catastrophic reset (SequenceReset_1 / ChannelReset_11):
    /// flips every per-symbol entry to Stale so the next snapshot cycle
    /// re-Healthifies symbols progressively.
    /// </summary>
    void OnSequenceReset();

    /// <summary>
    /// Fired once when all instrument definitions have been received and the
    /// channel transitions to Streaming. Used by managers to freeze metadata
    /// dictionaries (FreezeBooks / FreezeData).
    /// </summary>
    void OnInstrumentDefinitionsComplete(int instrumentCount);

    /// <summary>
    /// Called after all SBE messages in a UMDF packet have been dispatched.
    /// Used as a batch boundary for upstream conflation.
    /// </summary>
    void OnPacketProcessed() { }
}
