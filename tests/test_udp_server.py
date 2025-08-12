import socket
import threading
import time
from pathlib import Path

from server.src.udp_voice_server import UdpVoiceRelayServer
from server.src.protocol import AudioPacket, PCM16_CODEC


def run_server_background(ip: str, port: int) -> UdpVoiceRelayServer:
    server = UdpVoiceRelayServer(ip, port, app_name="test_voip_server")
    thread = threading.Thread(target=server.start, daemon=True)
    thread.start()
    # Wait for socket to bind
    time.sleep(0.2)
    return server


def build_packet(stream: int, seq: int, payload: bytes) -> bytes:
    pkt = AudioPacket(stream_id=stream, sequence=seq, timestamp_ms=int(time.time() * 1000), codec=PCM16_CODEC, payload=payload)
    return AudioPacket.encode(pkt)


def test_basic_relay():
    ip, port = "127.0.0.1", 50123
    server = run_server_background(ip, port)

    try:
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        receiver.bind((ip, 0))
        recv_addr = receiver.getsockname()

        # Join stream by sending first packet from both
        sender.sendto(build_packet(1, 1, b"hello"), (ip, port))
        receiver.sendto(build_packet(1, 1, b"join"), (ip, port))

        # Now send data from sender; expect receiver gets it
        data = build_packet(1, 2, b"voice")
        sender.sendto(data, (ip, port))

        receiver.settimeout(1)
        relayed, _ = receiver.recvfrom(2048)
        assert relayed.endswith(b"voice"), "Тест провален: Реципиент не получил ожидаемые данные"
        print("Тест успешно пройден")
    finally:
        server.stop()


