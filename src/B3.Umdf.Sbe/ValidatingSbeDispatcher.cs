using B3.Umdf.Mbo.Sbe.V16;

namespace B3.Umdf.Sbe;

/// <summary>
/// Reason a buffer was rejected by <see cref="ValidatingSbeDispatcher"/>.
/// </summary>
public enum SbeHeaderRejectReason
{
    /// <summary>Buffer was smaller than <see cref="MessageHeader.MESSAGE_SIZE"/>.</summary>
    HeaderTruncated,
    /// <summary>Header <c>SchemaId</c> did not match the expected value.</summary>
    SchemaIdMismatch,
    /// <summary>Header <c>Version</c> exceeded <see cref="ValidatingSbeDispatcher.MaxSupportedVersion"/>.</summary>
    VersionUnsupported,
    /// <summary>Header <c>BlockLength</c> exceeded the bytes remaining in the buffer.</summary>
    BlockLengthImplausible,
}

/// <summary>
/// Information passed to <see cref="ValidatingSbeDispatcher.OnHeaderMismatch"/> when the dispatcher
/// rejects a buffer. The <c>BufferLength</c> rather than the buffer itself is supplied because the
/// callback may be invoked from a hot ref-struct context.
/// </summary>
public readonly record struct SbeHeaderRejection(
    SbeHeaderRejectReason Reason,
    ushort SchemaId,
    ushort Version,
    ushort TemplateId,
    ushort BlockLength,
    int BufferLength);

/// <summary>
/// Defensive wrapper around the generated <see cref="SbeDispatcher"/> (issue #15).
/// Validates the SBE message header (SchemaId / Version / BlockLength) before delegating
/// to the generated dispatcher so a malformed or wrong-schema packet cannot drive the
/// codegen parser into an out-of-range slice.
///
/// Designed as a drop-in alternative — <see cref="Dispatch{T}"/> mirrors the signature of
/// <c>SbeDispatcher.Dispatch&lt;T&gt;</c> so callers can swap the call site to
/// <c>validator.Dispatch(buf, ref handler)</c> without further changes.
/// </summary>
public sealed class ValidatingSbeDispatcher
{
    /// <summary>SBE schema id for the B3 UMDF schema (b3-market-data-messages-2.2.0.xml, <c>id="2"</c>).</summary>
    public const ushort DefaultSchemaId = 2;
    /// <summary>Highest <c>sinceVersion</c> that the bundled <c>SbeSourceGenerator</c> generates code for.</summary>
    public const ushort DefaultMaxSupportedVersion = 16;

    /// <summary>Expected <c>SchemaId</c>. Headers carrying any other value are rejected.</summary>
    public ushort ExpectedSchemaId { get; }
    /// <summary>Inclusive upper bound on accepted header <c>Version</c>.</summary>
    public ushort MaxSupportedVersion { get; }
    /// <summary>When <c>true</c> (default), buffers that fail validation are skipped and not dispatched.</summary>
    public bool SkipOnMismatch { get; }
    /// <summary>When <c>true</c> (default), <c>BlockLength &gt; remaining-buffer</c> is treated as a mismatch.</summary>
    public bool ValidateBlockLength { get; }

    /// <summary>Optional callback invoked once per rejection — useful for logging or test assertions.</summary>
    public Action<SbeHeaderRejection>? OnHeaderMismatch { get; }

    public ValidatingSbeDispatcher(
        ushort expectedSchemaId = DefaultSchemaId,
        ushort maxSupportedVersion = DefaultMaxSupportedVersion,
        bool skipOnMismatch = true,
        bool validateBlockLength = true,
        Action<SbeHeaderRejection>? onHeaderMismatch = null)
    {
        ExpectedSchemaId = expectedSchemaId;
        MaxSupportedVersion = maxSupportedVersion;
        SkipOnMismatch = skipOnMismatch;
        ValidateBlockLength = validateBlockLength;
        OnHeaderMismatch = onHeaderMismatch;
    }

    /// <summary>
    /// Validates the SBE header at the start of <paramref name="buffer"/> and, if it passes,
    /// delegates to <see cref="SbeDispatcher.Dispatch{T}"/>. Returns <c>true</c> only if the
    /// buffer was successfully dispatched.
    /// </summary>
    public bool Dispatch<T>(ReadOnlySpan<byte> buffer, ref T handler)
        where T : struct, ISbeMessageHandler
    {
        if (!MessageHeader.TryReadHeader(buffer, out var blockLength, out var templateId, out var schemaId, out var version))
        {
            SbeValidationMetrics.HeaderTruncated.Add(1);
            OnHeaderMismatch?.Invoke(new SbeHeaderRejection(
                SbeHeaderRejectReason.HeaderTruncated, 0, 0, 0, 0, buffer.Length));
            return false;
        }

        if (schemaId != ExpectedSchemaId)
        {
            return Reject(SbeHeaderRejectReason.SchemaIdMismatch, buffer, ref handler,
                schemaId, version, templateId, blockLength);
        }

        if (version > MaxSupportedVersion)
        {
            return Reject(SbeHeaderRejectReason.VersionUnsupported, buffer, ref handler,
                schemaId, version, templateId, blockLength);
        }

        if (ValidateBlockLength)
        {
            int remaining = buffer.Length - MessageHeader.MESSAGE_SIZE;
            if (blockLength > remaining)
            {
                return Reject(SbeHeaderRejectReason.BlockLengthImplausible, buffer, ref handler,
                    schemaId, version, templateId, blockLength);
            }
        }

        return SbeDispatcher.Dispatch(buffer, ref handler);
    }

    private bool Reject<T>(
        SbeHeaderRejectReason reason,
        ReadOnlySpan<byte> buffer,
        ref T handler,
        ushort schemaId,
        ushort version,
        ushort templateId,
        ushort blockLength) where T : struct, ISbeMessageHandler
    {
        SbeValidationMetrics.HeaderMismatches.Add(1,
            new KeyValuePair<string, object?>("reason", reason.ToString()));
        OnHeaderMismatch?.Invoke(new SbeHeaderRejection(
            reason, schemaId, version, templateId, blockLength, buffer.Length));

        if (SkipOnMismatch)
            return false;

        return SbeDispatcher.Dispatch(buffer, ref handler);
    }
}
