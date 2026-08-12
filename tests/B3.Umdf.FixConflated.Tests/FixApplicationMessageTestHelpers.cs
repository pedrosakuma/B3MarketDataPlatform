using B3.Umdf.FixConflated;

namespace B3.Umdf.FixConflated.Tests;

internal static class FixApplicationMessageTestHelpers
{
    public static FixMessage RoundTrip(FixMessage message)
    {
        byte[] encoded = FixMessageCodec.Encode(message);
        var decoded = FixMessageCodec.Decode(encoded);
        Assert.True(decoded.Success);
        Assert.NotNull(decoded.Message);
        return decoded.Message!;
    }

    public static string GetRequired(FixMessage message, int tag)
    {
        Assert.True(message.TryGetString(tag, out string? value));
        return value!;
    }

    public static string[] GetAllValues(FixMessage message, int tag)
        => message.Fields.Where(f => f.Tag == tag).Select(f => f.Value).ToArray();
}
