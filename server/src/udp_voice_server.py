from __future__ import annotations

import socket
import threading
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, Tuple

from .logger_config import setup_logging
from .protocol import AudioPacket, default_recording_dir


@dataclass(slots=True)
class ClientInfo:
    address: Tuple[str, int]
    stream_id: int
    client_id: int | None = None
    display_name: str | None = None
    last_seq: int = -1
    # Last activity timestamp (monotonic seconds)
    last_seen: float | None = None


class UdpVoiceRelayServer:
    """
    Very small UDP relay. Clients send AudioPacket payloads to the server.
    Server re-broadcasts to other clients in the same stream.
    """

    def __init__(self, bind_ip: str, bind_port: int, app_name: str = "voip_server") -> None:
        self.bind_ip = bind_ip
        self.bind_port = bind_port
        self.logger = setup_logging(app_name)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Allow fast restart on Windows
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.bind_ip, self.bind_port))

        self.clients: Dict[Tuple[str, int], ClientInfo] = {}
        self.lock = threading.Lock()
        self.running = False
        self._presence_thread: threading.Thread | None = None

        self.record_dir: Path = default_recording_dir()

    def start(self) -> None:
        self.running = True
        self.logger.info("UDP Voice Relay running on %s:%d", self.bind_ip, self.bind_port)
        # Start presence broadcaster/maintenance thread
        self._presence_thread = threading.Thread(target=self._presence_loop, daemon=True)
        self._presence_thread.start()
        while self.running:
            try:
                data, addr = self.sock.recvfrom(2048)
            except OSError:
                break

            packet = AudioPacket.decode(data)
            if packet is None:
                self.logger.debug("Invalid packet from %s", addr)
                continue

            with self.lock:
                # Control message handling: detect control early for unknown addresses
                early_obj = None
                if packet.payload and packet.payload[0] == 1 and len(packet.payload) > 1:
                    try:
                        early_text = packet.payload[1:].decode("utf-8", errors="ignore")
                        early_obj = json.loads(early_text)
                    except Exception:
                        early_obj = None

                info = self.clients.get(addr)
                if info is None:
                    # If we received a 'leave' from an unknown socket, try to remove by logical id and do not create a new entry
                    if isinstance(early_obj, dict) and early_obj.get("t") == "leave":
                        req_id = None
                        try:
                            if early_obj.get("id") is not None:
                                req_id = int(early_obj.get("id"))
                        except Exception:
                            req_id = None
                        if req_id is not None:
                            removed_any = False
                            for other_addr, other in list(self.clients.items()):
                                if other.stream_id == packet.stream_id and other.client_id == req_id:
                                    self.clients.pop(other_addr, None)
                                    removed_any = True
                            if removed_any:
                                # Announce leave and refresh presence
                                self._broadcast_leave(packet.stream_id, req_id, None)
                                self._broadcast_presence(packet.stream_id)
                                continue
                    # If we received a presence request before register/join, answer without registering
                    if isinstance(early_obj, dict) and early_obj.get("t") in ("who", "presence_req"):
                        try:
                            self._send_presence_unicast(int(early_obj.get("stream", packet.stream_id)), addr)
                        except Exception:
                            pass
                        continue
                    # Register socket presence but do not announce until explicit join control arrives
                    info = ClientInfo(address=addr, stream_id=packet.stream_id)
                    self.clients[addr] = info
                    self.logger.info("New client %s joined stream %d", addr, packet.stream_id)
                info.last_seq = packet.sequence
                try:
                    import time as _t
                    info.last_seen = _t.monotonic()
                except Exception:
                    pass

                # Control message handling: payload[0] == 1, followed by UTF-8 JSON
                obj = None  # IMPORTANT: reset per-packet to avoid reusing previous control object
                if packet.payload:
                    kind = packet.payload[0]
                    if kind == 1 and len(packet.payload) > 1:
                        try:
                            text = packet.payload[1:].decode("utf-8", errors="ignore")
                            obj = json.loads(text)
                        except Exception:
                            obj = None
                if isinstance(obj, dict):
                            t = obj.get("t")
                            if t == "join":
                                info.client_id = int(obj.get("id")) if obj.get("id") is not None else None
                                name = obj.get("name")
                                info.display_name = name if isinstance(name, str) else None
                                info.stream_id = int(obj.get("stream", info.stream_id))
                                self.logger.info("Client join: id=%s name=%s addr=%s stream=%d", info.client_id, info.display_name, addr, info.stream_id)
                                # Deduplicate: drop any stale sessions with the same client_id but different address
                                if info.client_id is not None:
                                    for other_addr, other in list(self.clients.items()):
                                        if other_addr != addr and other.client_id == info.client_id and other.stream_id == info.stream_id:
                                            self.logger.info("Removing stale duplicate session: id=%s old_addr=%s", other.client_id, other_addr)
                                            self.clients.pop(other_addr, None)
                                            # Notify peers that the old session left
                                            self._broadcast_leave(info.stream_id, other.client_id, other.display_name)
                                # Send ack to joiner and immediately unicast current presence list
                                try:
                                    ack = {"t": "ack", "stream": info.stream_id}
                                    payload = b"\x01" + json.dumps(ack, ensure_ascii=False).encode("utf-8")
                                    data_ack = AudioPacket.encode(AudioPacket(stream_id=info.stream_id, sequence=0, timestamp_ms=0, codec=0, payload=payload))
                                    self.sock.sendto(data_ack, addr)
                                    # Unicast presence snapshot to the joiner for faster UI update
                                    self._send_presence_unicast(info.stream_id, addr)
                                except OSError:
                                    pass
                                # Announce join and refresh presence (send presence twice to reduce UDP loss)
                                self._broadcast_join(info)
                                self._broadcast_presence(info.stream_id)
                                try:
                                    self._broadcast_presence(info.stream_id)
                                except Exception:
                                    pass
                            elif t == "leave":
                                # Remove by address and by provided logical id if present
                                req_id = None
                                try:
                                    if obj.get("id") is not None:
                                        req_id = int(obj.get("id"))
                                except Exception:
                                    req_id = None
                                self.logger.info("Client leave: req_id=%s current_id=%s addr=%s", req_id, info.client_id, addr)
                                # Remove current address
                                self.clients.pop(addr, None)
                                # Also remove any other records in the same stream with the same logical id
                                if req_id is not None:
                                    for other_addr, other in list(self.clients.items()):
                                        if other.stream_id == packet.stream_id and other.client_id == req_id:
                                            self.clients.pop(other_addr, None)
                                # Announce leave and refresh presence
                                self._broadcast_leave(packet.stream_id, req_id or info.client_id, info.display_name)
                                # Send presence twice to reduce UDP loss risk
                                self._broadcast_presence(packet.stream_id)
                                try:
                                    self._broadcast_presence(packet.stream_id)
                                except Exception:
                                    pass
                                continue
                            elif t in ("who", "presence_req"):
                                # Unicast current presence snapshot to requester
                                try:
                                    self._send_presence_unicast(info.stream_id, addr)
                                except Exception:
                                    pass

                # Relay to all clients in same stream (exclude sender)
                for other_addr, other in list(self.clients.items()):
                    if other_addr == addr or other.stream_id != packet.stream_id:
                        continue
                    try:
                        self.sock.sendto(data, other_addr)
                    except OSError as e:
                        self.logger.warning("Failed to relay to %s: %s", other_addr, e)

    def stop(self) -> None:
        self.running = False
        try:
            self.sock.close()
        finally:
            self.logger.info("Server stopped")
        if self._presence_thread is not None:
            try:
                self._presence_thread.join(timeout=0.5)
            except RuntimeError:
                pass

    def _broadcast_presence(self, stream_id: int) -> None:
        data = self._build_presence_packet(stream_id)
        for addr, c in list(self.clients.items()):
            if c.stream_id != stream_id:
                continue
            try:
                self.sock.sendto(data, addr)
            except OSError as e:
                self.logger.warning("Failed to send presence to %s: %s", addr, e)

    def _build_presence_packet(self, stream_id: int) -> bytes:
        members = []
        for c in self.clients.values():
            if c.stream_id != stream_id:
                continue
            if c.client_id is None:
                continue
            members.append({
                "id": c.client_id,
                "name": c.display_name or (str(c.client_id)),
            })
        obj = {"t": "presence", "stream": stream_id, "members": members}
        payload = b"\x01" + json.dumps(obj, ensure_ascii=False).encode("utf-8")
        pkt = AudioPacket(stream_id=stream_id, sequence=0, timestamp_ms=0, codec=0, payload=payload)
        return AudioPacket.encode(pkt)

    def _send_presence_unicast(self, stream_id: int, addr: Tuple[str, int]) -> None:
        data = self._build_presence_packet(stream_id)
        try:
            self.sock.sendto(data, addr)
        except OSError as e:
            self.logger.debug("Failed to unicast presence to %s: %s", addr, e)

    def _broadcast_join(self, info: ClientInfo) -> None:
        """Notify peers in the same stream that someone joined."""
        obj = {
            "t": "join",
            "id": info.client_id,
            "name": info.display_name or (str(info.client_id) if info.client_id is not None else str(info.address)),
            "stream": info.stream_id,
        }
        self._send_control_to_stream(info.stream_id, obj)

    def _broadcast_leave(self, stream_id: int, client_id: int | None, name: str | None) -> None:
        """Notify peers in the same stream that someone left."""
        obj = {
            "t": "leave",
            "id": client_id,
            "name": name or (str(client_id) if client_id is not None else ""),
            "stream": stream_id,
        }
        self._send_control_to_stream(stream_id, obj)

    def _send_control_to_stream(self, stream_id: int, obj: dict) -> None:
        payload = b"\x01" + json.dumps(obj, ensure_ascii=False).encode("utf-8")
        pkt = AudioPacket(stream_id=stream_id, sequence=0, timestamp_ms=0, codec=0, payload=payload)
        data = AudioPacket.encode(pkt)
        for addr, c in list(self.clients.items()):
            if c.stream_id != stream_id:
                continue
            try:
                self.sock.sendto(data, addr)
            except OSError:
                pass

    def _presence_loop(self) -> None:
        import time
        TIMEOUT_SEC = 6.0
        while self.running:
            time.sleep(3)
            if not self.running:
                break
            # Timeout stale clients and periodically resend presence
            with self.lock:
                now = time.monotonic()
                # Collect removals per stream
                removed: dict[int, list[ClientInfo]] = {}
                for addr, c in list(self.clients.items()):
                    if c.last_seen is not None and now - c.last_seen > TIMEOUT_SEC:
                        self.logger.info("Client timed out: id=%s addr=%s", c.client_id, addr)
                        self.clients.pop(addr, None)
                        removed.setdefault(c.stream_id, []).append(c)

                streams = {c.stream_id for c in self.clients.values()} | set(removed.keys())

            # Broadcast leaves for removed clients then update presence per stream
            for sid, removed_list in removed.items():
                for c in removed_list:
                    try:
                        self._broadcast_leave(sid, c.client_id, c.display_name)
                    except Exception:
                        pass

            for sid in streams:
                try:
                    self._broadcast_presence(sid)
                except Exception as e:
                    self.logger.debug("Presence broadcast error: %s", e)


def run_server(ip: str = "0.0.0.0", port: int = 50010) -> None:
    server = UdpVoiceRelayServer(ip, port)
    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == "__main__":
    run_server()


