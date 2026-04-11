using B3.Umdf.Transport;

namespace B3.Umdf.PcapReplay;

public sealed record PcapChannelSource(string FilePath, ChannelType Channel, int Group = 0);
