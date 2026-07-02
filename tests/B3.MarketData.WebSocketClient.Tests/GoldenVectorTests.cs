using System;
using System.IO;
using System.Text.Json;
using B3.MarketData.Wire;

namespace B3.MarketData.WebSocketClient.Tests;

/// <summary>
/// Cross-language forward-compatibility contract: decode the committed,
/// implementation-independent golden vectors in <c>tests/golden/wire-v2-vectors.json</c>
/// with the production <see cref="WireFormat"/> readers and assert the decoded
/// fields. The JS half of this contract lives in
/// <c>tests/golden/decode.test.mjs</c>; both decode the identical hex bytes.
/// </summary>
public class GoldenVectorTests
{
    private static JsonDocument LoadGolden()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, "tests", "golden", "wire-v2-vectors.json");
            if (File.Exists(candidate))
                return JsonDocument.Parse(File.ReadAllText(candidate));
            dir = dir.Parent;
        }
        throw new FileNotFoundException("Could not locate tests/golden/wire-v2-vectors.json above " + AppContext.BaseDirectory);
    }

    private static byte[] HexToBytes(string hex)
    {
        var bytes = new byte[hex.Length / 2];
        for (int i = 0; i < bytes.Length; i++)
            bytes[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);
        return bytes;
    }

    [Fact]
    public void GoldenHeaderConstants_Match()
    {
        using var doc = LoadGolden();
        Assert.Equal(2, doc.RootElement.GetProperty("protocolVersion").GetInt32());
        Assert.Equal(WireV2.HeaderSize, doc.RootElement.GetProperty("headerSize").GetInt32());
    }

    [Fact]
    public void AllGoldenVectors_DecodeToExpectedFields()
    {
        using var doc = LoadGolden();
        var receivedUtc = DateTime.UtcNow;

        foreach (var vec in doc.RootElement.GetProperty("vectors").EnumerateArray())
        {
            string name = vec.GetProperty("name").GetString()!;
            string type = vec.GetProperty("type").GetString()!;
            var bytes = HexToBytes(vec.GetProperty("hex").GetString()!);
            var expected = vec.GetProperty("expected");

            Assert.True(WireFrame.TryReadHeader(bytes, out uint length, out var msgType, out var headerFlags),
                $"{name}: header must parse");
            Assert.Equal((uint)bytes.Length, length);
            Assert.Equal(HeaderFlags.None, headerFlags);

            var payload = bytes.AsSpan(WireV2.HeaderSize, (int)length - WireV2.HeaderSize);

            switch (type)
            {
                case "SubscribeOk":
                {
                    var (secId, flags, symbol) = WireFormat.ReadSubscribeOk(payload);
                    Assert.Equal(ExpectedU64(expected, "securityId"), secId);
                    Assert.Equal((uint)expected.GetProperty("flags").GetInt64(), flags);
                    Assert.Equal(expected.GetProperty("symbol").GetString(), symbol);
                    break;
                }
                case "OrderAdded":
                {
                    var (secId, orderId, side, price, qty) = WireFormat.ReadOrderEvent(payload);
                    Assert.Equal(ExpectedU64(expected, "securityId"), secId);
                    Assert.Equal(ExpectedU64(expected, "orderId"), orderId);
                    Assert.Equal(expected.GetProperty("side").GetInt64(), side);
                    Assert.Equal(expected.GetProperty("price").GetInt64(), price);
                    Assert.Equal(expected.GetProperty("qty").GetInt64(), qty);
                    break;
                }
                case "Trade":
                {
                    var (secId, price, qty, tradeId, flags) = WireFormat.ReadTrade(payload);
                    Assert.Equal(ExpectedU64(expected, "securityId"), secId);
                    Assert.Equal(expected.GetProperty("price").GetInt64(), price);
                    Assert.Equal(expected.GetProperty("qty").GetInt64(), qty);
                    Assert.Equal(expected.GetProperty("tradeId").GetInt64(), tradeId);
                    Assert.Equal((byte)expected.GetProperty("flags").GetInt64(), (byte)flags);
                    break;
                }
                case "MarketTierUpdate":
                {
                    var (secId, side, totalQty, orderCount) = WireFormat.ReadMarketTierUpdate(payload);
                    Assert.Equal(ExpectedU64(expected, "securityId"), secId);
                    Assert.Equal(expected.GetProperty("side").GetInt64(), side);
                    Assert.Equal(expected.GetProperty("qty").GetInt64(), totalQty);
                    Assert.Equal(expected.GetProperty("count").GetInt64(), orderCount);
                    break;
                }
                case "LevelUpdate":
                {
                    var (secId, side, price, totalQty, orderCount) = WireFormat.ReadLevelUpdate(payload);
                    Assert.Equal(ExpectedU64(expected, "securityId"), secId);
                    Assert.Equal(expected.GetProperty("side").GetInt64(), side);
                    Assert.Equal(expected.GetProperty("price").GetInt64(), price);
                    Assert.Equal(expected.GetProperty("qty").GetInt64(), totalQty);
                    Assert.Equal(expected.GetProperty("count").GetInt64(), orderCount);
                    break;
                }
                case "LevelDeleted":
                {
                    var (secId, side, price) = WireFormat.ReadLevelDeleted(payload);
                    Assert.Equal(ExpectedU64(expected, "securityId"), secId);
                    Assert.Equal(expected.GetProperty("side").GetInt64(), side);
                    Assert.Equal(expected.GetProperty("price").GetInt64(), price);
                    break;
                }
                case "ServerStatus":
                {
                    Assert.Equal(expected.GetProperty("ready").GetBoolean(), WireFormat.ReadServerStatus(payload));
                    break;
                }
                case "InfoSnapshot":
                {
                    var ev = WireFormat.ReadInfoSnapshot(payload, "PETR4", receivedUtc);
                    Assert.Equal(ExpectedU64(expected, "securityId"), ev.SecurityId);
                    var fields = expected.GetProperty("fields");
                    // JS returns raw i64; C# scales price fields by PriceScale (1e4).
                    Assert.Equal(fields.GetProperty("OpeningPrice").GetInt64() / WireFormat.PriceScale, ev.OpeningPrice);
                    Assert.Equal(fields.GetProperty("HighPrice").GetInt64() / WireFormat.PriceScale, ev.HighPrice);
                    Assert.Equal(fields.GetProperty("LowPrice").GetInt64() / WireFormat.PriceScale, ev.LowPrice);
                    break;
                }
                case "ServerHello":
                {
                    var (protocolVersion, capabilities, buildVersion) = WireFormat.ReadServerHello(payload);
                    Assert.Equal((uint)expected.GetProperty("protocolVersion").GetInt64(), protocolVersion);
                    Assert.Equal((uint)expected.GetProperty("capabilities").GetInt64(), (uint)capabilities);
                    Assert.Equal(expected.GetProperty("buildVersion").GetString(), buildVersion);
                    break;
                }
                default:
                    Assert.Fail($"Unhandled golden vector type: {type} ({name})");
                    break;
            }
        }
    }

    private static ulong ExpectedU64(JsonElement expected, string key) =>
        ulong.Parse(expected.GetProperty(key).GetString()!);
}
