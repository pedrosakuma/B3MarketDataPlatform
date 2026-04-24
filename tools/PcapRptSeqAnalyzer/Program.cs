using System.Collections.Generic;
using System.Runtime.InteropServices;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.PcapReplay;

namespace B3.Umdf.Tools.PcapRptSeqAnalyzer;

/// <summary>
/// Decode-only PCAP analyzer to validate B3 UMDF rptSeq protocol invariants:
///   #1 — rptSeq is monotonic per SecurityID across ALL message templates
///   #4 — Within a single packet, rptSeqs for the same SecurityID are monotonic
/// No book/state simulation; pure protocol verification.
/// </summary>
internal static class Program
{
    private const int SbeHeaderSize = 8;          // blockLength(2) + templateId(2) + schemaId(2) + version(2)
    private const int FramingHeaderSize = 4;
    private const int PacketHeaderSize = 16;

    private static int Main(string[] args)
    {
        if (args.Length < 1)
        {
            Console.Error.WriteLine("Usage: pcap-rptseq-analyzer <pcap-path> [--max-packets N] [--security-id ID] [--top N] [--print-violations N]");
            return 1;
        }

        string path = args[0];
        long maxPackets = long.MaxValue;
        ulong? targetSecId = null;
        int top = 10;
        int printViolations = 50;

        for (int i = 1; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--max-packets": maxPackets = long.Parse(args[++i]); break;
                case "--security-id": targetSecId = ulong.Parse(args[++i]); break;
                case "--top": top = int.Parse(args[++i]); break;
                case "--print-violations": printViolations = int.Parse(args[++i]); break;
            }
        }

        Console.WriteLine($"Reading {path}");
        var lastRptSeq = new Dictionary<ulong, (uint RptSeq, ushort Template, uint PktSeq)>();
        var perSecCount = new Dictionary<ulong, long>();
        var perSecPerTplCount = new Dictionary<(ulong Sec, ushort Tpl), long>();
        var perTplCount = new Dictionary<ushort, long>();
        var perTplWithRptSeq = new Dictionary<ushort, long>();

        long globalGapsAcrossTemplates = 0;     // monotonic violation per-secId across all templates
        long crossTemplateAdvances = 0;         // advances where the kind differs from previous (= proves cross-kind sharing)
        long intraPacketViolations = 0;         // same-secId rptSeq not monotonic within a single packet
        long pktCount = 0;
        long sbeMsgCount = 0;
        long rptSeqMsgCount = 0;

        var violationLog = new List<string>();
        var crossLog = new List<string>();
        var intraLog = new List<string>();

        // Per-packet tracking: secId -> (lastRpt, lastTpl)
        var inPacket = new Dictionary<ulong, (uint RptSeq, ushort Tpl)>(64);

        using var reader = new MmapPcapReader(path);
        int udpOffset = -1;

        while (reader.TryReadNext(out var pkt) && pktCount < maxPackets)
        {
            pktCount++;
            var frame = pkt.Data.Span;
            if (udpOffset < 0)
                udpOffset = UdpExtractor.ComputeUdpPayloadOffset(frame, reader.LinkType);
            if (udpOffset >= frame.Length)
                continue;
            var payload = frame[udpOffset..];
            if (payload.Length < PacketHeaderSize)
                continue;

            if (!UmdfPacketHeaderTryParse(payload, out byte channelNumber, out uint packetSeqNum))
                continue;

            inPacket.Clear();

            int offset = PacketHeaderSize;
            while (offset + FramingHeaderSize + SbeHeaderSize <= payload.Length)
            {
                ushort msgLen = MemoryMarshal.Read<ushort>(payload[offset..]); // FramingHeader.MessageLength (LE)
                if (msgLen < FramingHeaderSize + SbeHeaderSize) break;
                if (offset + msgLen > payload.Length) break;

                var sbeSlice = payload[(offset + FramingHeaderSize)..(offset + msgLen)];
                ushort blockLength = MemoryMarshal.Read<ushort>(sbeSlice);
                ushort templateId = MemoryMarshal.Read<ushort>(sbeSlice[2..]);
                var body = sbeSlice[SbeHeaderSize..];

                sbeMsgCount++;
                perTplCount[templateId] = perTplCount.GetValueOrDefault(templateId) + 1;

                if (TryExtract(templateId, body, blockLength, out ulong secId, out uint? rptSeqOpt))
                {
                    if (targetSecId.HasValue && secId != targetSecId.Value)
                        goto next;

                    perSecCount[secId] = perSecCount.GetValueOrDefault(secId) + 1;
                    perSecPerTplCount[(secId, templateId)] = perSecPerTplCount.GetValueOrDefault((secId, templateId)) + 1;

                    if (rptSeqOpt is uint rptSeq)
                    {
                        rptSeqMsgCount++;
                        perTplWithRptSeq[templateId] = perTplWithRptSeq.GetValueOrDefault(templateId) + 1;

                        // Cross-template monotonicity
                        if (lastRptSeq.TryGetValue(secId, out var prev))
                        {
                            if (rptSeq <= prev.RptSeq)
                            {
                                globalGapsAcrossTemplates++;
                                if (violationLog.Count < printViolations)
                                    violationLog.Add($"  pkt#{pktCount} pktSeq={packetSeqNum} secId={secId} tpl={templateId} rptSeq={rptSeq} <= prev rptSeq={prev.RptSeq} (prevTpl={prev.Template}, prevPktSeq={prev.PktSeq})");
                            }
                            else if (prev.Template != templateId)
                            {
                                crossTemplateAdvances++;
                                if (crossLog.Count < printViolations)
                                    crossLog.Add($"  secId={secId} prevTpl={prev.Template} prevRpt={prev.RptSeq} → tpl={templateId} rpt={rptSeq} (gap={rptSeq - prev.RptSeq - 1})");
                            }
                        }
                        lastRptSeq[secId] = (rptSeq, templateId, packetSeqNum);

                        // Intra-packet monotonicity
                        if (inPacket.TryGetValue(secId, out var pp))
                        {
                            if (rptSeq <= pp.RptSeq)
                            {
                                intraPacketViolations++;
                                if (intraLog.Count < printViolations)
                                    intraLog.Add($"  pktSeq={packetSeqNum} secId={secId} tpl={templateId} rptSeq={rptSeq} <= earlier-in-packet rpt={pp.RptSeq} tpl={pp.Tpl}");
                            }
                        }
                        inPacket[secId] = (rptSeq, templateId);
                    }
                }

            next:
                offset += msgLen;
            }
        }

        Console.WriteLine();
        Console.WriteLine("=== Summary ===");
        Console.WriteLine($"packets read           : {pktCount:N0}");
        Console.WriteLine($"sbe messages           : {sbeMsgCount:N0}");
        Console.WriteLine($"messages with rptSeq   : {rptSeqMsgCount:N0}");
        Console.WriteLine($"distinct secIds        : {perSecCount.Count:N0}");
        Console.WriteLine($"distinct templates     : {perTplCount.Count:N0}");
        Console.WriteLine();
        Console.WriteLine($"INVARIANT #1 (per-secId rptSeq monotonic across templates):");
        Console.WriteLine($"  violations           : {globalGapsAcrossTemplates:N0}");
        Console.WriteLine($"  cross-template advances (proves rptSeq is shared across kinds): {crossTemplateAdvances:N0}");
        Console.WriteLine($"INVARIANT #4 (intra-packet rptSeq monotonic per secId):");
        Console.WriteLine($"  violations           : {intraPacketViolations:N0}");
        Console.WriteLine();

        Console.WriteLine($"=== Top {top} templates ===");
        foreach (var (tpl, cnt) in perTplCount.OrderByDescending(kv => kv.Value).Take(top))
        {
            long withRpt = perTplWithRptSeq.GetValueOrDefault(tpl);
            Console.WriteLine($"  tpl={tpl,3} count={cnt,12:N0} withRptSeq={withRpt,12:N0}");
        }

        Console.WriteLine();
        Console.WriteLine($"=== Top {top} secIds by message count ===");
        foreach (var (sec, cnt) in perSecCount.OrderByDescending(kv => kv.Value).Take(top))
        {
            var perTpl = perSecPerTplCount.Where(kv => kv.Key.Sec == sec)
                .OrderByDescending(kv => kv.Value)
                .Select(kv => $"tpl{kv.Key.Tpl}={kv.Value}")
                .Take(8);
            Console.WriteLine($"  secId={sec,12} total={cnt,10:N0}  [{string.Join(", ", perTpl)}]");
        }

        if (violationLog.Count > 0)
        {
            Console.WriteLine();
            Console.WriteLine($"=== First {violationLog.Count} INVARIANT #1 violations ===");
            foreach (var v in violationLog) Console.WriteLine(v);
        }
        if (crossLog.Count > 0)
        {
            Console.WriteLine();
            Console.WriteLine($"=== First {crossLog.Count} cross-template advances (sample) ===");
            foreach (var v in crossLog) Console.WriteLine(v);
        }
        if (intraLog.Count > 0)
        {
            Console.WriteLine();
            Console.WriteLine($"=== First {intraLog.Count} INVARIANT #4 violations ===");
            foreach (var v in intraLog) Console.WriteLine(v);
        }

        return 0;
    }

    private static bool UmdfPacketHeaderTryParse(ReadOnlySpan<byte> payload, out byte channelNumber, out uint packetSeqNum)
    {
        // PacketHeader: channel(1) + reserved(1) + sequenceVersion(2) + sequenceNumber(4) + sendingTime(8) = 16
        channelNumber = payload[0];
        packetSeqNum = MemoryMarshal.Read<uint>(payload[4..]);
        return true;
    }

    /// <summary>
    /// Extracts (SecurityID, RptSeq?) from any SBE message body that carries them.
    /// Returns false for messages without SecurityID. RptSeq may be null for templates that don't carry it
    /// (e.g. SequenceReset_1, ChannelReset_11) or for snapshot bodies (Trade in snapshot has rptSeq=0).
    /// </summary>
    private static bool TryExtract(ushort templateId, ReadOnlySpan<byte> body, ushort blockLength, out ulong secId, out uint? rptSeq)
    {
        secId = 0;
        rptSeq = null;
        switch (templateId)
        {
            case Order_MBO_50Data.MESSAGE_ID:
                if (!Order_MBO_50Data.TryParse(body, blockLength, out var m50)) return false;
                secId = (ulong)m50.Data.SecurityID;
                rptSeq = m50.Data.RptSeq is { } r50 ? (uint)r50 : null;
                return true;
            case DeleteOrder_MBO_51Data.MESSAGE_ID:
                if (!DeleteOrder_MBO_51Data.TryParse(body, blockLength, out var m51)) return false;
                secId = (ulong)m51.Data.SecurityID;
                rptSeq = m51.Data.RptSeq is { } r51 ? (uint)r51 : null;
                return true;
            case MassDeleteOrders_MBO_52Data.MESSAGE_ID:
                if (!MassDeleteOrders_MBO_52Data.TryParse(body, blockLength, out var m52)) return false;
                secId = (ulong)m52.Data.SecurityID;
                rptSeq = m52.Data.RptSeq is { } r52 ? (uint)r52 : null;
                return true;
            case Trade_53Data.MESSAGE_ID:
                if (!Trade_53Data.TryParse(body, blockLength, out var m53)) return false;
                secId = (ulong)m53.Data.SecurityID;
                rptSeq = m53.Data.RptSeq is { } r53 ? (uint)r53 : null;
                return true;
            case ForwardTrade_54Data.MESSAGE_ID:
                if (!ForwardTrade_54Data.TryParse(body, blockLength, out var m54)) return false;
                secId = (ulong)m54.Data.SecurityID;
                rptSeq = m54.Data.RptSeq is { } r54 ? (uint)r54 : null;
                return true;
            case ExecutionSummary_55Data.MESSAGE_ID:
                if (!ExecutionSummary_55Data.TryParse(body, blockLength, out var m55)) return false;
                secId = (ulong)m55.Data.SecurityID;
                rptSeq = m55.Data.RptSeq is { } r55 ? (uint)r55 : null;
                return true;
            case ExecutionStatistics_56Data.MESSAGE_ID:
                if (!ExecutionStatistics_56Data.TryParse(body, blockLength, out var m56)) return false;
                secId = (ulong)m56.Data.SecurityID;
                rptSeq = m56.Data.RptSeq is { } r56 ? (uint)r56 : null;
                return true;
            case TradeBust_57Data.MESSAGE_ID:
                if (!TradeBust_57Data.TryParse(body, blockLength, out var m57)) return false;
                secId = (ulong)m57.Data.SecurityID;
                rptSeq = m57.Data.RptSeq is { } r57 ? (uint)r57 : null;
                return true;
            case SecurityStatus_3Data.MESSAGE_ID:
                if (!SecurityStatus_3Data.TryParse(body, blockLength, out var m3)) return false;
                secId = (ulong)m3.Data.SecurityID;
                rptSeq = m3.Data.RptSeq is { } r3 ? (uint)r3 : null;
                return true;
            case OpeningPrice_15Data.MESSAGE_ID:
                if (!OpeningPrice_15Data.TryParse(body, blockLength, out var m15)) return false;
                secId = (ulong)m15.Data.SecurityID;
                rptSeq = m15.Data.RptSeq is { } r15 ? (uint)r15 : null;
                return true;
            case TheoreticalOpeningPrice_16Data.MESSAGE_ID:
                if (!TheoreticalOpeningPrice_16Data.TryParse(body, blockLength, out var m16)) return false;
                secId = (ulong)m16.Data.SecurityID;
                rptSeq = m16.Data.RptSeq is { } r16 ? (uint)r16 : null;
                return true;
            case ClosingPrice_17Data.MESSAGE_ID:
                if (!ClosingPrice_17Data.TryParse(body, blockLength, out var m17)) return false;
                secId = (ulong)m17.Data.SecurityID;
                rptSeq = m17.Data.RptSeq is { } r17 ? (uint)r17 : null;
                return true;
            case AuctionImbalance_19Data.MESSAGE_ID:
                if (!AuctionImbalance_19Data.TryParse(body, blockLength, out var m19)) return false;
                secId = (ulong)m19.Data.SecurityID;
                rptSeq = m19.Data.RptSeq is { } r19 ? (uint)r19 : null;
                return true;
            case QuantityBand_21Data.MESSAGE_ID:
                if (!QuantityBand_21Data.TryParse(body, blockLength, out var m21)) return false;
                secId = (ulong)m21.Data.SecurityID;
                rptSeq = m21.Data.RptSeq is { } r21 ? (uint)r21 : null;
                return true;
            case PriceBand_22Data.MESSAGE_ID:
                if (!PriceBand_22Data.TryParse(body, blockLength, out var m22)) return false;
                secId = (ulong)m22.Data.SecurityID;
                rptSeq = m22.Data.RptSeq is { } r22 ? (uint)r22 : null;
                return true;
            case HighPrice_24Data.MESSAGE_ID:
                if (!HighPrice_24Data.TryParse(body, blockLength, out var m24)) return false;
                secId = (ulong)m24.Data.SecurityID;
                rptSeq = m24.Data.RptSeq is { } r24 ? (uint)r24 : null;
                return true;
            case LowPrice_25Data.MESSAGE_ID:
                if (!LowPrice_25Data.TryParse(body, blockLength, out var m25)) return false;
                secId = (ulong)m25.Data.SecurityID;
                rptSeq = m25.Data.RptSeq is { } r25 ? (uint)r25 : null;
                return true;
            case LastTradePrice_27Data.MESSAGE_ID:
                if (!LastTradePrice_27Data.TryParse(body, blockLength, out var m27)) return false;
                secId = (ulong)m27.Data.SecurityID;
                rptSeq = m27.Data.RptSeq is { } r27 ? (uint)r27 : null;
                return true;
            case SettlementPrice_28Data.MESSAGE_ID:
                if (!SettlementPrice_28Data.TryParse(body, blockLength, out var m28)) return false;
                secId = (ulong)m28.Data.SecurityID;
                rptSeq = m28.Data.RptSeq is { } r28 ? (uint)r28 : null;
                return true;
            case OpenInterest_29Data.MESSAGE_ID:
                if (!OpenInterest_29Data.TryParse(body, blockLength, out var m29)) return false;
                secId = (ulong)m29.Data.SecurityID;
                rptSeq = m29.Data.RptSeq is { } r29 ? (uint)r29 : null;
                return true;
            default:
                return false;
        }
    }
}
