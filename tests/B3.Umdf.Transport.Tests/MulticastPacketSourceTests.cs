using System.Net;

namespace B3.Umdf.Transport.Tests;

public class MulticastPacketSourceTests
{
    [Fact]
    public void BuildSourceMembershipRequest_UsesKernelFieldOrder()
    {
        var multicastGroup = IPAddress.Parse("239.10.10.10");
        var localAddress = IPAddress.Parse("10.1.2.3");
        var sourceAddress = IPAddress.Parse("10.9.8.7");

        var request = MulticastPacketSource.BuildSourceMembershipRequest(multicastGroup, localAddress, sourceAddress);

        Assert.Equal(multicastGroup.GetAddressBytes(), request[..4]);
        Assert.Equal(localAddress.GetAddressBytes(), request[4..8]);
        Assert.Equal(sourceAddress.GetAddressBytes(), request[8..12]);
    }
}
