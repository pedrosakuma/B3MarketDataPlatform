#!/usr/bin/env python3
"""
Empirically determine SO_REUSEPORT semantics for multicast UDP on this kernel.

Creates N receiver sockets all bound to the same (multicast group, port),
joins the multicast group on each, and sends M packets via a separate sender.
Counts received packets per receiver.

Outcomes:
- Each receiver gets ~M packets  -> kernel DELIVERS A COPY TO EACH
                                    (REUSEPORT does NOT load-balance multicast)
- Receivers split ~M/N each       -> kernel DOES load-balance
"""
import socket
import struct
import sys
import threading
import time

MCAST_GRP = "239.10.99.1"
MCAST_PORT = 39911
N_RECEIVERS = 4
N_PACKETS = 1000
PACKET_SIZE = 100

def make_receiver(idx, counts):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    SO_REUSEPORT = 15
    try:
        s.setsockopt(socket.SOL_SOCKET, SO_REUSEPORT, 1)
    except OSError as e:
        print(f"[recv {idx}] SO_REUSEPORT failed: {e}", file=sys.stderr)
    s.bind(("", MCAST_PORT))
    mreq = struct.pack("=4sl", socket.inet_aton(MCAST_GRP), socket.INADDR_ANY)
    s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    s.settimeout(2.0)
    received = 0
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            data, _ = s.recvfrom(2048)
            received += 1
        except socket.timeout:
            break
    counts[idx] = received
    s.close()

def main():
    counts = [0] * N_RECEIVERS
    threads = [threading.Thread(target=make_receiver, args=(i, counts), daemon=True)
               for i in range(N_RECEIVERS)]
    for t in threads:
        t.start()
    # give receivers time to bind/join
    time.sleep(0.5)

    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sender.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
    sender.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
    payload = b"x" * PACKET_SIZE
    for i in range(N_PACKETS):
        sender.sendto(payload, (MCAST_GRP, MCAST_PORT))
        if i % 100 == 0:
            time.sleep(0.001)  # avoid overflowing kernel buffer
    sender.close()
    print(f"sent {N_PACKETS} packets")

    for t in threads:
        t.join(timeout=6.0)

    print("\nReceived per socket:")
    for i, c in enumerate(counts):
        print(f"  socket {i}: {c} packets ({100.0*c/N_PACKETS:.1f}% of sent)")
    total = sum(counts)
    print(f"  TOTAL:    {total} packets (= {total/N_PACKETS:.2f}x sent)")

    avg_per_socket = total / N_RECEIVERS
    if avg_per_socket > 0.8 * N_PACKETS:
        print("\n>>> CONCLUSION: kernel DELIVERS A COPY to each socket.")
        print("    SO_REUSEPORT does NOT load-balance multicast on this kernel.")
        print("    ReceiveSocketCount > 1 multiplies CPU cost without throughput gain.")
    elif avg_per_socket < 0.4 * N_PACKETS:
        print("\n>>> CONCLUSION: kernel LOAD-BALANCES across sockets.")
        print("    SO_REUSEPORT distributes multicast (unexpected).")
    else:
        print("\n>>> CONCLUSION: ambiguous; investigate further.")

if __name__ == "__main__":
    main()
