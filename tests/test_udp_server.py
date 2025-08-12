import socket
import threading
import time
from pathlib import Path

from server.src.udp_voice_server import UdpVoiceRelayServer
from server.src.protocol import AudioPacket, PCM16_CODEC
import json


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

        # Proper control join (kind=1 + JSON) from both sockets
        join_sender = b"\x01" + json.dumps({"t": "join", "id": 1001, "name": "sender", "stream": 1}).encode("utf-8")
        join_receiver = b"\x01" + json.dumps({"t": "join", "id": 1002, "name": "receiver", "stream": 1}).encode("utf-8")
        sender.sendto(build_packet(1, 1, join_sender), (ip, port))
        receiver.sendto(build_packet(1, 1, join_receiver), (ip, port))

        # Now send data from sender; expect receiver gets it
        # Audio payload (kind=0 + bytes)
        data = build_packet(1, 2, b"\x00voice")
        sender.sendto(data, (ip, port))

        receiver.settimeout(1.5)
        header_size = 14
        got_audio = False
        end_time = time.time() + 1.5
        while time.time() < end_time:
            try:
                relayed, _ = receiver.recvfrom(4096)
            except socket.timeout:
                break
            if len(relayed) <= header_size:
                continue
            payload = relayed[header_size:]
            if not payload:
                continue
            # kind=0 -> audio
            if payload[0] == 0 and relayed.endswith(b"voice"):
                got_audio = True
                break
        assert got_audio, "Тест провален: Реципиент не получил ожидаемые данные"
        print("Тест успешно пройден")
    finally:
        server.stop()


