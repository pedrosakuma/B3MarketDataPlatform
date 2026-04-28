using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using B3.Umdf.Book;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// Allocation-regression sentry for
/// <see cref="MarketDataManager"/> on the steady SecurityDefinition
/// re-broadcast path. Before P11-2, every SecurityDefinition_12 reception
/// unconditionally re-allocated 6 strings (Symbol/Asset/Group/etc.), 4
/// handler delegates (NoUnderlyings/NoLegs/NoInstrAttribs/SecurityDesc),
/// a closure for ReadGroups, and ran BumpVersion() — even though the
/// InstrDef channel re-broadcasts the SAME definition every few seconds
/// (the exchange only bumps SecurityValidityTimestamp on real changes).
/// Profiling at 5x replay attributed ~31 % of all sampled allocations to
/// this method.
///
/// The fix caches <c>SecurityValidityTimestamp</c> on
/// <c>InstrumentInfo</c>; matching values short-circuit the entire body
/// before any allocation. This test asserts that 10 000 repeats of the
/// same SecDef payload allocate well under 4 KB total (after a single
/// warmup parse).
/// </summary>
[Collection(nameof(AllocationSensitiveCollection))]
public class MarketDataManagerSecDefAllocationTests
{
    [Fact]
    public void HandleSecurityDefinition_RebroadcastSameTimestamp_IsZeroAlloc()
    {
        const ulong sec = 12345;
        const long validityTs = 1_700_000_000L;
        const int repeats = 10_000;

        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var mdm = new MarketDataManager(stateRegistry: registry);

        var packet = BuildSecDefPacket(sec, validityTsTime: validityTs);

        // Warmup: first call MUST parse fully (no cached value yet).
        mdm.OnPacket(in EmptyPacket, packet, SecurityDefinition_12Data.MESSAGE_ID);
        Assert.Equal(0L, mdm.SecurityDefinitionsSkipped);
        Assert.True(mdm.InstrumentData.TryGetValue(sec, out var info));
        Assert.Equal((ulong)validityTs, info!.LastSecurityValidityTimestamp);

        GC.Collect(2, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, blocking: true);

        long beforeBytes = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < repeats; i++)
            mdm.OnPacket(in EmptyPacket, packet, SecurityDefinition_12Data.MESSAGE_ID);
        long afterBytes = GC.GetAllocatedBytesForCurrentThread();

        long deltaBytes = afterBytes - beforeBytes;

        Assert.Equal((long)repeats, mdm.SecurityDefinitionsSkipped);

        // Threshold of 80 B/call (800 KB total) tolerates ~48 B/call of
        // residual dispatch-pipeline overhead measured experimentally
        // (MessageHeader parse + `out` reader struct copy + Interlocked
        // bookkeeping), but is still ~80x below the pre-fix per-call cost
        // (~6.4 KB/call: 6 strings + 4 handler delegates + closure + lists
        // + BumpVersion). Any regression that re-enables those allocations
        // will blow the threshold by orders of magnitude.
        long threshold = 80L * repeats;
        Assert.True(deltaBytes < threshold,
            $"HandleSecurityDefinition rebroadcast path allocated {deltaBytes} bytes across {repeats} calls " +
            $"(={(double)deltaBytes / repeats:F3} B/call). Threshold is {threshold} bytes total. " +
            $"Suspect the SecurityValidityTimestamp early-out regressed (P11-2).");
    }

    [Fact]
    public void HandleSecurityDefinition_DifferentTimestamp_BypassesEarlyOut()
    {
        const ulong sec = 67890;
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var mdm = new MarketDataManager(stateRegistry: registry);

        var p1 = BuildSecDefPacket(sec, validityTsTime: 1_700_000_000L);
        var p2 = BuildSecDefPacket(sec, validityTsTime: 1_700_000_500L);

        mdm.OnPacket(in EmptyPacket, p1, SecurityDefinition_12Data.MESSAGE_ID);
        mdm.OnPacket(in EmptyPacket, p1, SecurityDefinition_12Data.MESSAGE_ID); // skipped
        mdm.OnPacket(in EmptyPacket, p2, SecurityDefinition_12Data.MESSAGE_ID); // re-parse
        mdm.OnPacket(in EmptyPacket, p2, SecurityDefinition_12Data.MESSAGE_ID); // skipped

        Assert.Equal(2L, mdm.SecurityDefinitionsSkipped);
        Assert.True(mdm.InstrumentData.TryGetValue(sec, out var info));
        Assert.Equal(1_700_000_500UL, info!.LastSecurityValidityTimestamp);
    }

    [Fact]
    public void HandleSecurityDefinition_NullTimestamp_ParsesEveryTime()
    {
        const ulong sec = 11111;
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var mdm = new MarketDataManager(stateRegistry: registry);

        // long.MinValue is the SBE null sentinel for UTCTimestampSeconds.Time.
        // Defensive fall-through: must NOT skip when timestamp is null,
        // otherwise upstream payloads with absent timestamps would silently
        // freeze instrument metadata.
        var packet = BuildSecDefPacket(sec, validityTsTime: long.MinValue);

        mdm.OnPacket(in EmptyPacket, packet, SecurityDefinition_12Data.MESSAGE_ID);
        mdm.OnPacket(in EmptyPacket, packet, SecurityDefinition_12Data.MESSAGE_ID);
        mdm.OnPacket(in EmptyPacket, packet, SecurityDefinition_12Data.MESSAGE_ID);

        Assert.Equal(0L, mdm.SecurityDefinitionsSkipped);
    }

    private static readonly UmdfPacket EmptyPacket = new()
    {
        Data = ReadOnlyMemory<byte>.Empty,
        Channel = ChannelType.InstrumentDefinition,
        ChannelGroup = 1,
        ReceivedTimestampTicks = 0,
    };

    /// <summary>
    /// Builds a complete SBE message: 8-byte SBE header + fixed block + 3
    /// empty group headers + empty varData header. Uses the generated
    /// comprehensive <c>SecurityDefinition_12Data.TryEncode</c> overload so
    /// the wire layout (group-header sizes, varData prefix) stays in sync
    /// with the schema, then patches the SecurityValidityTimestamp via
    /// <see cref="Unsafe.As{TFrom,TTo}"/> reinterpret of the underlying
    /// <c>long time</c> field.
    /// </summary>
    private static byte[] BuildSecDefPacket(ulong securityId, long validityTsTime)
    {
        const int sbeHeaderSize = 8;
        // Reinterpret the raw long as UTCTimestampSeconds (struct is just
        // `[StructLayout(Sequential, Pack=1)] long time`). This bypasses the
        // missing public setter on `time` and lets us inject any value
        // including the null sentinel `long.MinValue`.
        UTCTimestampSeconds ts = Unsafe.As<long, UTCTimestampSeconds>(ref validityTsTime);
        var payload = new SecurityDefinition_12Data
        {
            SecurityID = (SecurityID)securityId,
            SecurityValidityTimestamp = ts,
        };

        var buf = new byte[1024];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(0), (ushort)SecurityDefinition_12Data.MESSAGE_SIZE);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), SecurityDefinition_12Data.MESSAGE_ID);

        bool ok = SecurityDefinition_12Data.TryEncode(
            payload,
            buf.AsSpan(sbeHeaderSize),
            ReadOnlySpan<SecurityDefinition_12Data.NoUnderlyingsData>.Empty,
            ReadOnlySpan<SecurityDefinition_12Data.NoLegsData>.Empty,
            ReadOnlySpan<SecurityDefinition_12Data.NoInstrAttribsData>.Empty,
            ReadOnlySpan<byte>.Empty,
            out int bytesWritten);
        Assert.True(ok, "TryEncode failed");

        var result = new byte[sbeHeaderSize + bytesWritten];
        buf.AsSpan(0, result.Length).CopyTo(result);
        return result;
    }
}
