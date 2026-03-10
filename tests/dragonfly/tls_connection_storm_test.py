"""
TLS Connection Storm Defense Test

This test verifies the integrity of the 3-stage TLS pre-filter defense mechanisms
(TCP_DEFER_ACCEPT, the Stage 2+3 connection peeks).

It simulates a Zombie/Half-TLS connection storm by flooding the server with thousands
of zombie connections across multiple CPU cores, followed by validating that
legitimate clients can still successfully connect and execute commands.

NOTE: This is NOT a performance benchmark. It is a functional survival/regression test.
Because Python's asyncio event loop introduces significant client-side latency under
massive concurrent load, this test is highly sensitive to CPU starvation. It must
strictly run on `large` or `xlarge` CI runners to avoid false-positive failures.

Manual Execution:
You can run this locally:
> cd tests
> source ~/venvs/dragonfly/bin/activate
> pytest -s -v dragonfly/tls_connection_storm_test.py -o asyncio_default_fixture_loop_scope=function -x
"""

import asyncio
import multiprocessing
import os
import socket
import ssl
import statistics
import time
from collections import Counter

import pytest
from .utility import *


def run_storm_worker(target_count, host, port, ca_cert_path, mode):
    """
    Executes a high-concurrency connection flood in a separate OS process.
    Bypassing the Python GIL via multiprocessing is necessary to genuinely
    overwhelm the server's kernel-level TCP accept queue. This
    """
    import asyncio
    import socket
    import ssl
    import struct

    async def _worker():
        # Disable standard TLS verifications to maximize connection generation speed.
        # Note: this disables policy, not crypto operations. We just disable some veridfications on client side which are not mandatory for the test.
        ctx = ssl.create_default_context(cafile=ca_cert_path)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        # Limit concurrent outgoing connections per process to avoid local ephemeral port exhaustion.
        semaphore = asyncio.Semaphore(200)

        async def stress_connection():
            async with semaphore:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.setblocking(False)
                    loop = asyncio.get_running_loop()
                    await loop.sock_connect(sock, (host, port))

                    # Stage 2/3 Defense Trigger: Send the 2-byte TLS magic header and stall,
                    # or send nothing at all (Pure Zombie) to trigger Stage 1 (TCP_DEFER_ACCEPT).
                    if mode == "half_tls":
                        await loop.sock_sendall(sock, b"\x16\x03")

                    # Hold the connection open to simulate a stalled client hoarding server fibers.
                    await asyncio.sleep(0.1)

                    # Trigger a hard RST (TCP Reset) to aggressively tear down the socket
                    # without the standard TIME_WAIT handshake, maximizing kernel stress.
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                    sock.close()
                except Exception:
                    # Expected under extreme load; local network buffers will overflow.
                    pass

        tasks = [stress_connection() for _ in range(target_count)]
        await asyncio.gather(*tasks)

    asyncio.run(_worker())


# Certificates and keys are pre-generated on helio
CERT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../helio/util/tls/certificates")
)
CA_CERT = os.path.join(CERT_DIR, "ca-cert.pem")
SERVER_CERT = os.path.join(CERT_DIR, "server-cert.pem")
SERVER_KEY = os.path.join(CERT_DIR, "server-key.pem")
CLIENT_CERT = os.path.join(CERT_DIR, "client-cert.pem")
CLIENT_KEY = os.path.join(CERT_DIR, "client-key.pem")


class TestTlsConnectionStorm:
    @pytest.mark.large
    async def test_tls_mixed_storm(self, df_factory):
        # Elevate file descriptor limits so the test process itself isn't the bottleneck
        # when generating thousands of concurrent sockets.
        import resource

        resource.setrlimit(resource.RLIMIT_NOFILE, (65535, 65535))

        server = df_factory.create(
            tls=True, tls_ca_cert_file=CA_CERT, tls_cert_file=SERVER_CERT, tls_key_file=SERVER_KEY
        )
        server.start()

        host = "127.0.0.1"
        port = server.port
        ca_cert = CA_CERT

        num_malicious_clients = 10000
        num_worker_processes = 4
        clients_per_worker = num_malicious_clients // num_worker_processes
        num_valid_clients = 500

        print(
            f"\n[!] Spawning {num_worker_processes} processes"
            f" to generate {num_malicious_clients} malicious connections..."
        )
        print(f"[!] Main process will attempt {num_valid_clients} valid PINGs...")

        start_time = time.time()

        # Distribute the attack load across multiple CPU cores, alternating between
        # pure connection stalling and Half-TLS stalling to test all filter stages.
        pool = multiprocessing.Pool(processes=num_worker_processes)
        modes = ["pure_zombie", "half_tls"]
        worker_pool_result = pool.starmap_async(
            run_storm_worker,
            [
                (clients_per_worker, host, port, ca_cert, modes[i % 2])
                for i in range(num_worker_processes)
            ],
        )

        # Block until the attack phase finishes. The storm overwhelms the TCP accept queue.
        # Good clients must connect AFTER the storm drains, proving the server recovers promptly.
        worker_pool_result.get(timeout=30)
        pool.close()
        pool.join()

        storm_duration = time.time() - start_time
        print(f"Storm completed in {storm_duration:.2f}s. Launching valid clients...")

        # Allow the server a brief window to process the final RST packets and evict
        # the remaining zombies from the accept queue.
        await asyncio.sleep(0.5)

        valid_latencies = []
        errors = []

        # Limit legitimate client concurrency so we measure server processing latency,
        # not local client-side asyncio bottlenecking.
        sem = asyncio.Semaphore(50)

        async def valid_client():
            async with sem:
                t0 = time.time()
                try:
                    # Use raw asyncio SSL sockets to guarantee a pure, unfragmented
                    # 0x16 TLS ClientHello on the wire.
                    ctx = ssl.create_default_context(cafile=ca_cert)
                    ctx.load_cert_chain(certfile=CLIENT_CERT, keyfile=CLIENT_KEY)
                    ctx.check_hostname = False

                    reader, writer = await asyncio.open_connection(
                        host, port, ssl=ctx, server_hostname=host
                    )

                    # Send a raw RESP PING command through the established TLS tunnel
                    writer.write(b"*1\r\n$4\r\nPING\r\n")
                    await writer.drain()

                    response = await asyncio.wait_for(reader.read(100), timeout=5)
                    if b"PONG" in response:
                        valid_latencies.append((time.time() - t0) * 1000)

                    writer.close()
                    await writer.wait_closed()
                except Exception as e:
                    errors.append(f"{type(e).__name__}: {e}")

        await asyncio.gather(*(valid_client() for _ in range(num_valid_clients)))

        total_duration = time.time() - start_time

        print(f"\n=== RESULTS (Malicious: {num_malicious_clients}, Valid: {num_valid_clients}) ===")
        print(f"Total Test Duration: {total_duration:.2f} seconds")
        print(f"Throughput (Est): {num_malicious_clients / total_duration:.0f} conn/sec")

        if errors:
            print("\n=== VALID CLIENT ERRORS ===")
            for err, count in Counter(errors).items():
                print(f"  {count}x {err}")

        # If zero good clients succeeded, the Half-TLS attack successfully caused
        # fiber exhaustion and permanently locked the server.
        assert len(valid_latencies) > 0, "FATAL: 0 Valid Clients succeeded! Server locked up."

        p50 = sorted(valid_latencies)[int(len(valid_latencies) * 0.50)]
        p99 = sorted(valid_latencies)[int(len(valid_latencies) * 0.99)]
        avg = statistics.mean(valid_latencies)

        print(f"Avg Latency: {avg:.2f} ms")
        print(f"P50 Latency: {p50:.2f} ms")
        print(f"P99 Latency: {p99:.2f} ms")
        print(f"Max Latency: {max(valid_latencies):.2f} ms")
        print("===================================================\n")

        # Validate the Stage 3 defense successfully prevented OpenSSL fiber exhaustion.
        # A generous threshold (500ms) is used because Python's asyncio event loop
        # introduces significant artificial latency under heavy concurrent load.
        assert p50 < 500, f"Server degraded or hung! P50 latency during storm: {p50:.2f}ms"
