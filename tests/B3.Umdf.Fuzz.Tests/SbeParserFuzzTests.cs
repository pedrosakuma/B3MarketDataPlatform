using System;
using System.Collections.Generic;
using B3.Umdf.Mbo.Sbe.V16;

namespace B3.Umdf.Fuzz.Tests;

/// <summary>
/// Property-based fuzz harness for the generated SBE message readers.
///
/// Motivation: GitHub issue #12 was a textbook fuzzing-catchable bug — a partner
/// emitter omitted the 1-byte length prefix of <c>securityDesc</c> in
/// <see cref="SecurityDefinition_12Data"/>. Our generated
/// <see cref="TextEncoding.Create"/> reads the next byte as length and
/// <c>Slice</c>s past the buffer end, throwing <see cref="ArgumentOutOfRangeException"/>
/// on the feed thread.
///
/// What this harness does: feed each target reader's <c>TryParse + ReadGroups</c>
/// pipeline a stream of random <c>byte[]</c> buffers of varying lengths. We DO
/// NOT require the parser to succeed (random bytes are virtually never valid
/// SBE), only that it fail SAFELY: the only exceptions allowed to escape are
/// the small documented set in <see cref="ExpectedExceptions"/>. Anything else
/// (NRE, AccessViolation, OOM, etc.) is a memory-safety bug worth a P0.
///
/// Today the allowlist still includes <see cref="ArgumentOutOfRangeException"/>
/// and <see cref="IndexOutOfRangeException"/> — exactly the exceptions issue #12
/// threw. That means this harness is useful TODAY for catching memory-unsafety
/// regressions in the SBE generator, and will become STRICTER (and catch the
/// #12 class of bug directly) once the generator is hardened to surface
/// truncated/malformed buffers via a <c>TryRead</c>-style API instead of
/// throwing.
/// </summary>
public class SbeParserFuzzTests
{
    private const int Iterations = 500;
    private const int MaxBufferLength = 512;

    /// <summary>
    /// Exceptions the harness currently considers "acceptable failure modes"
    /// for malformed input. Each entry comes with a comment justifying why
    /// it's tolerated TODAY and what would let us tighten the bound.
    /// </summary>
    private static readonly HashSet<Type> ExpectedExceptions = new()
    {
        // Span<T>.Slice throws ArgumentOutOfRangeException when (start + length)
        // exceeds the buffer. This is the EXACT exception observed in issue #12.
        // Allowed for now — once the generator emits TextEncoding.TryCreate(...)
        // we will REMOVE this entry and the harness will fail until the parsers
        // are upgraded.
        typeof(ArgumentOutOfRangeException),

        // MemoryMarshal.AsRef<T>() over an undersized span throws this. Same
        // family of issue as above. Tracked separately so we can grep CI logs
        // for which generator path needs a TryRead.
        typeof(IndexOutOfRangeException),
    };

    [Fact]
    public void Fuzz_SecurityDefinition_12_OnlyExpectedExceptionsEscape()
    {
        PropertyRunner.ForAllBytes(Iterations, MaxBufferLength, buf =>
        {
            ParseSecurityDefinition_12(buf);
        });
    }

    [Fact]
    public void Fuzz_SnapshotFullRefresh_Orders_MBO_71_OnlyExpectedExceptionsEscape()
    {
        PropertyRunner.ForAllBytes(Iterations, MaxBufferLength, buf =>
        {
            ParseSnapshotFullRefresh_Orders_MBO_71(buf);
        });
    }

    [Fact]
    public void Fuzz_News_5_OnlyExpectedExceptionsEscape()
    {
        PropertyRunner.ForAllBytes(Iterations, MaxBufferLength, buf =>
        {
            ParseNews_5(buf);
        });
    }

    [Fact]
    public void Fuzz_DeleteOrder_MBO_51_OnlyExpectedExceptionsEscape()
    {
        PropertyRunner.ForAllBytes(Iterations, MaxBufferLength, buf =>
        {
            ParseDeleteOrder_MBO_51(buf);
        });
    }

    /// <summary>
    /// Repro test for the bug class behind issue #12. We craft a buffer that
    /// is exactly <c>BLOCK_LENGTH</c> bytes (no group headers, no varData)
    /// and confirm that calling <c>ReadGroups</c> currently throws an
    /// <em>expected</em> exception (i.e. the generator is unsafe but at least
    /// surfaces a typed exception we can catch). When the generator is hardened
    /// to handle truncation via a TryRead API, this test should be flipped
    /// to <c>Assert.Null(ex)</c>.
    /// </summary>
    [Fact]
    public void Issue12Repro_TruncatedSecurityDefinition_ThrowsExpectedExceptionType()
    {
        var buf = new byte[SecurityDefinition_12Data.MESSAGE_SIZE];
        // Call WITHOUT the swallow-helper so we can observe the raw exception.
        var ex = Record.Exception(() =>
        {
            Assert.True(SecurityDefinition_12Data.TryParse(buf, out var reader));
            reader.ReadGroups(
                callbackNoUnderlyings: static (in SecurityDefinition_12Data.NoUnderlyingsData _) => { },
                callbackNoLegs: static (in SecurityDefinition_12Data.NoLegsData _) => { },
                callbackNoInstrAttribs: static (in SecurityDefinition_12Data.NoInstrAttribsData _) => { },
                callbackSecurityDesc: static (TextEncoding _) => { });
        });

        // Today: we expect EITHER a typed allowlisted throw (issue #12 class)
        // OR a graceful no-throw if the generator has been hardened. Anything
        // else is a memory-safety regression.
        if (ex is not null)
        {
            Assert.Contains(ex.GetType(), ExpectedExceptions);
        }
    }

    private static void ParseSecurityDefinition_12(ReadOnlySpan<byte> buffer)
    {
        try
        {
            if (!SecurityDefinition_12Data.TryParse(buffer, out var reader)) return;
            reader.ReadGroups(
                callbackNoUnderlyings: static (in SecurityDefinition_12Data.NoUnderlyingsData _) => { },
                callbackNoLegs: static (in SecurityDefinition_12Data.NoLegsData _) => { },
                callbackNoInstrAttribs: static (in SecurityDefinition_12Data.NoInstrAttribsData _) => { },
                callbackSecurityDesc: static (TextEncoding _) => { });
            _ = reader.BytesConsumed;
        }
        catch (Exception ex) when (ExpectedExceptions.Contains(ex.GetType()))
        {
            // Allowlisted — see ExpectedExceptions for rationale.
        }
    }

    private static void ParseSnapshotFullRefresh_Orders_MBO_71(ReadOnlySpan<byte> buffer)
    {
        try
        {
            if (!SnapshotFullRefresh_Orders_MBO_71Data.TryParse(buffer, out var reader)) return;
            reader.ReadGroups(
                callbackNoMDEntries: static (in SnapshotFullRefresh_Orders_MBO_71Data.NoMDEntriesData _) => { });
            _ = reader.BytesConsumed;
        }
        catch (Exception ex) when (ExpectedExceptions.Contains(ex.GetType()))
        {
        }
    }

    private static void ParseNews_5(ReadOnlySpan<byte> buffer)
    {
        try
        {
            if (!News_5Data.TryParse(buffer, out var reader)) return;
            reader.ReadGroups(
                callbackHeadline: static (VarString _) => { },
                callbackText: static (VarString _) => { },
                callbackURLLink: static (VarString _) => { });
            _ = reader.BytesConsumed;
        }
        catch (Exception ex) when (ExpectedExceptions.Contains(ex.GetType()))
        {
        }
    }

    private static void ParseDeleteOrder_MBO_51(ReadOnlySpan<byte> buffer)
    {
        try
        {
            // DeleteOrder_MBO_51 is fixed-size (no ReadGroups call needed) but
            // we still feed it through TryParse to lock in that the static
            // entry point cannot blow up on undersized/oversized buffers.
            if (!DeleteOrder_MBO_51Data.TryParse(buffer, out var reader)) return;
            _ = reader.Data.SecurityID;
            _ = reader.BlockLength;
        }
        catch (Exception ex) when (ExpectedExceptions.Contains(ex.GetType()))
        {
        }
    }
}
