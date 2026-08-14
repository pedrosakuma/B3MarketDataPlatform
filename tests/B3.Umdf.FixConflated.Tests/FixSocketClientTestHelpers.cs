using System.IO.Compression;
using System.Net.Sockets;
using System.Text;
using B3.Umdf.FixConflated;
using Xunit.Sdk;

namespace B3.Umdf.FixConflated.Tests;

internal static class FixSocketClientTestHelpers
{
    public static async Task<byte[]> ReadRawBytesAsync(NetworkStream stream, int minimumBytes, TimeSpan? timeout = null)
    {
        byte[] buffer = new byte[Math.Max(256, minimumBytes)];
        int buffered = 0;
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(5));

        while (buffered < minimumBytes)
        {
            if (buffered == buffer.Length)
                Array.Resize(ref buffer, buffer.Length * 2);

            int read = await stream.ReadAsync(buffer.AsMemory(buffered), cts.Token);
            Assert.True(read > 0, "Expected the FIX server to send bytes before closing the socket.");
            buffered += read;
        }

        return buffer[..buffered];
    }

    public static FixMessage InflateSingleMessage(byte[] compressedBytes, out string inflatedText)
    {
        using var input = new MemoryStream(compressedBytes);
        using var zlib = FixZlibCompression.CreateDecompressionStream(input);
        using var output = new MemoryStream();
        zlib.CopyTo(output);
        byte[] inflated = output.ToArray();
        inflatedText = Encoding.ASCII.GetString(inflated);

        FixDecodeResult decoded = FixMessageCodec.Decode(inflated);
        Assert.True(decoded.Success, $"Expected inflated FIX frame to decode successfully, got {decoded.Error}.");
        Assert.Equal(inflated.Length, decoded.BytesConsumed);
        return decoded.Message!;
    }

    public sealed class InflatingFixClient : IAsyncDisposable
    {
        private readonly NetworkStream _stream;
        private readonly Stream _inflateStream;
        private byte[] _buffer = new byte[4096];
        private int _buffered;

        public InflatingFixClient(NetworkStream stream)
        {
            _stream = stream;
            _inflateStream = FixZlibCompression.CreateDecompressionStream(stream, leaveOpen: true);
        }

        public Task SendAsync(FixMessage message)
            => _stream.WriteAsync(FixMessageCodec.Encode(message)).AsTask();

        public async Task<FixMessage> ReadMessageAsync(TimeSpan? timeout = null)
        {
            using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(5));
            return await ReadMessageAsync(cts.Token).ConfigureAwait(false);
        }

        public async Task<FixMessage> ReadMessageAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                FixDecodeResult decoded = FixMessageCodec.Decode(_buffer.AsSpan(0, _buffered));
                if (decoded.Success)
                {
                    FixMessage message = decoded.Message!;
                    Consume(decoded.BytesConsumed);
                    return message;
                }

                if (decoded.Error != FixDecodeError.Incomplete)
                    throw new XunitException($"Expected a full FIX frame but decode failed with {decoded.Error}.");

                EnsureCapacity();
                int read = await _inflateStream.ReadAsync(_buffer.AsMemory(_buffered), cancellationToken);
                Assert.True(read > 0, "Expected the FIX server to send a FIX frame before closing the socket.");
                _buffered += read;
            }
        }

        public async Task AssertClosedWithoutFrameAsync(TimeSpan? timeout = null)
        {
            using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(2));

            try
            {
                while (true)
                {
                    FixDecodeResult decoded = FixMessageCodec.Decode(_buffer.AsSpan(0, _buffered));
                    if (decoded.Success)
                    {
                        FixMessage unexpected = decoded.Message!;
                        throw new XunitException(
                            $"Expected stale reconnect to be dropped without Logout, but received MsgType={FixApplicationMessageTestHelpers.GetRequired(unexpected, FixTags.MsgType)}.");
                    }

                    if (decoded.Error != FixDecodeError.Incomplete)
                        throw new XunitException($"Expected transport close without FIX frame, but decode failed with {decoded.Error}.");

                    EnsureCapacity();
                    int read = await _inflateStream.ReadAsync(_buffer.AsMemory(_buffered), cts.Token);
                    if (read == 0)
                    {
                        Assert.Equal(0, _buffered);
                        return;
                    }

                    _buffered += read;
                }
            }
            catch (OperationCanceledException) when (!cts.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                throw new XunitException("Expected the FIX server to close the stale reconnect promptly, but it stayed open past the timeout.");
            }
            catch (IOException)
            {
                Assert.Equal(0, _buffered);
            }
            catch (SocketException)
            {
                Assert.Equal(0, _buffered);
            }
        }

        public ValueTask DisposeAsync()
        {
            _inflateStream.Dispose();
            return ValueTask.CompletedTask;
        }

        private void Consume(int count)
        {
            Buffer.BlockCopy(_buffer, count, _buffer, 0, _buffered - count);
            _buffered -= count;
        }

        private void EnsureCapacity()
        {
            if (_buffered < _buffer.Length)
                return;

            Array.Resize(ref _buffer, _buffer.Length * 2);
        }
    }
}
