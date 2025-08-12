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
        # Main operational logger (rotating)
        self.logger = setup_logging(app_name)
        # Detailed diagnostics into a separate rotating file without console spam
        self.detail_logger = setup_logging(app_name + "_detail", include_console=False, level=10)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Allow fast restart on Windows
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            # Increase receive buffer to handle bursts of media chunks
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 << 20)
            # Increase send buffer to reduce drops under burst forwarding
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4 << 20)
        except OSError:
            pass
        self.sock.bind((self.bind_ip, self.bind_port))

        self.clients: Dict[Tuple[str, int], ClientInfo] = {}
        self.lock = threading.Lock()
        self.running = False
        self._presence_thread: threading.Thread | None = None

        self.record_dir: Path = default_recording_dir()
        # Simple in-memory chat history per stream (list of control JSON strings)
        # Store only chat and compact media signals (media_end) for replay (no media chunks!).
        self.chat_history: Dict[int, list[str]] = {}
        # Sampling step for media logs to avoid flooding
        self._media_log_sample_step = 200
        # Track which media mids were already recorded in history per stream to avoid duplicates on join replay
        self._history_seen_media_mids: Dict[int, set[str]] = {}

    def start(self) -> None:
        self.running = True
        self.logger.info("UDP Voice Relay running on %s:%d", self.bind_ip, self.bind_port)
        # Start presence broadcaster/maintenance thread
        self._presence_thread = threading.Thread(target=self._presence_loop, daemon=True)
        self._presence_thread.start()
        while self.running:
            try:
                data, addr = self.sock.recvfrom(65535)
            except OSError as e:
                # Do not terminate server loop on sporadic socket errors (e.g., buffer pressure)
                try:
                    self.logger.debug("recvfrom error: %s; continuing", e)
                except Exception:
                    pass
                continue

            try:
                packet = AudioPacket.decode(data)
                if packet is None:
                    self.logger.debug("Invalid packet from %s", addr)
                    continue

                with self.lock:
                    # Control message handling and client map access mostly below; keep lock minimal here
                    pass
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
                    self.logger.info("New socket %s seen on stream %d (awaiting join)", addr, packet.stream_id)
                info.last_seq = packet.sequence
                try:
                    import time as _t
                    info.last_seen = _t.monotonic()
                except Exception:
                    pass

                # Control message handling: payload[0] == 1, followed by UTF-8 JSON
                obj = None  # IMPORTANT: reset per-packet to avoid reusing previous control object
                control_type: str | None = None
                sender_logical_id: int | None = None
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
                            control_type = t if isinstance(t, str) else None
                            try:
                                if obj.get("id") is not None:
                                    sender_logical_id = int(obj.get("id"))
                            except Exception:
                                sender_logical_id = None
                            try:
                                # Compact structured details for diagnostics without huge payloads
                                if control_type == "chat":
                                    txt = obj.get("text")
                                    self.detail_logger.debug(
                                        "CTRL chat: len=%s id=%s name=%s stream=%s addr=%s",
                                        len(txt) if isinstance(txt, str) else None,
                                        sender_logical_id,
                                        obj.get("name"),
                                        obj.get("stream"),
                                        addr,
                                    )
                                elif control_type == "media":
                                    # Sample media chunk logs (first, last, and every Nth)
                                    idx_val = obj.get("index")
                                    tot_val = obj.get("total")
                                    try:
                                        idx_i = int(idx_val) if idx_val is not None else None
                                        tot_i = int(tot_val) if tot_val is not None else None
                                    except Exception:
                                        idx_i, tot_i = None, None
                                    if idx_i is None or tot_i is None or idx_i == 0 or (tot_i is not None and idx_i == max(tot_i - 1, 0)) or (idx_i % self._media_log_sample_step == 0):
                                        data_len = len(obj.get("data")) if isinstance(obj.get("data"), str) else None
                                        self.detail_logger.debug(
                                            "CTRL media: mid=%s idx=%s total=%s mtype=%s mime=%s data_len=%s id=%s stream=%s addr=%s",
                                            obj.get("mid"), idx_val, tot_val, obj.get("mtype"), obj.get("mime"), data_len,
                                            sender_logical_id, obj.get("stream"), addr,
                                        )
                                elif control_type == "media_end":
                                    self.detail_logger.debug(
                                        "CTRL media_end: mid=%s total=%s id=%s stream=%s addr=%s",
                                        obj.get("mid"), obj.get("total"), sender_logical_id, obj.get("stream"), addr,
                                    )
                                elif control_type == "media_req":
                                    need = obj.get("need")
                                    need_len = len(need) if isinstance(need, list) else None
                                    need_preview = need[:20] if isinstance(need, list) else None
                                    self.detail_logger.debug(
                                        "CTRL media_req: mid=%s need_len=%s need_head=%s id=%s stream=%s addr=%s",
                                        obj.get("mid"), need_len, need_preview, sender_logical_id, obj.get("stream"), addr,
                                    )
                                else:
                                    self.detail_logger.debug(
                                        "CTRL %s from id=%s stream=%s addr=%s",
                                        control_type, sender_logical_id, obj.get("stream"), addr,
                                    )
                            except Exception:
                                pass
                            if t == "join":
                                info.client_id = int(obj.get("id")) if obj.get("id") is not None else None
                                name = obj.get("name")
                                info.display_name = name if isinstance(name, str) else None
                                info.stream_id = int(obj.get("stream", info.stream_id))
                                self.logger.info("Client join: id=%s name=%s addr=%s stream=%d", info.client_id, info.display_name, addr, info.stream_id)
                                try:
                                    self.detail_logger.debug("Active clients in stream %s: %s", info.stream_id, [a for a, c in self.clients.items() if c.stream_id == info.stream_id])
                                except Exception:
                                    pass
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
                                    # Replay chat history to the joiner
                                    try:
                                        hist = list(self.chat_history.get(info.stream_id, []))
                                        self.detail_logger.debug("Replaying history: count=%d to addr=%s stream=%s", len(hist[-300:]), addr, info.stream_id)
                                        for item in hist[-300:]:  # limit
                                            payload_hist = b"\x01" + item.encode("utf-8", errors="ignore")
                                            data_hist = AudioPacket.encode(AudioPacket(stream_id=info.stream_id, sequence=0, timestamp_ms=0, codec=0, payload=payload_hist))
                                            self.sock.sendto(data_hist, addr)
                                    except Exception:
                                        pass
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
                            elif t in ("chat", "media"):
                                # Only chat is stored in history; media chunks are not stored to avoid replay flood
                                if t == "chat":
                                    try:
                                        s = json.dumps(obj, ensure_ascii=False)
                                    except Exception:
                                        s = None
                                    if s is not None:
                                        lst = self.chat_history.setdefault(info.stream_id, [])
                                        lst.append(s)
                                        if len(lst) > 2000:
                                            del lst[: len(lst) - 2000]
                                    # Unicast chat_ack to sender (server received)
                                    try:
                                        ack = {"t": "chat_ack", "ts": obj.get("ts"), "id": sender_logical_id, "stream": info.stream_id}
                                        payload_ack = b"\x01" + json.dumps(ack, ensure_ascii=False).encode("utf-8")
                                        data_ack = AudioPacket.encode(AudioPacket(stream_id=info.stream_id, sequence=0, timestamp_ms=0, codec=0, payload=payload_ack))
                                        self.sock.sendto(data_ack, addr)
                                        # Send a second ack shortly after to reduce UDP loss impact
                                        def _resend_ack_later(data_bytes: bytes, target: tuple[str, int]):
                                            import time as _t
                                            try:
                                                _t.sleep(0.03)
                                                self.sock.sendto(data_bytes, target)
                                            except Exception:
                                                pass
                                        threading.Thread(target=_resend_ack_later, args=(data_ack, addr), daemon=True).start()
                                    except Exception:
                                        pass
                                # Sanity: warn if JSON 'stream' field mismatches header stream
                                try:
                                    js_stream = int(obj.get("stream")) if obj.get("stream") is not None else None
                                except Exception:
                                    js_stream = None
                                if js_stream is not None and js_stream != packet.stream_id:
                                    self.logger.debug(
                                        "Control stream mismatch: header=%s json=%s from id=%s addr=%s",
                                        packet.stream_id, js_stream, sender_logical_id, addr,
                                    )
                                # Proactively forward control to peers now (without waiting for raw packet relay)
                                try:
                                    recipients_now = self._send_control_to_stream_excluding(info.stream_id, obj, addr)
                                    # Lightweight duplicate after a short delay to reduce packet loss impact
                                    if t == "chat":
                                        def _replay_control_later(stream: int, obj_copy: dict, exclude: tuple[str, int]):
                                            import time as _t
                                            try:
                                                _t.sleep(0.02)
                                                self._send_control_to_stream_excluding(stream, obj_copy, exclude)
                                            except Exception:
                                                pass
                                        threading.Thread(target=_replay_control_later, args=(info.stream_id, dict(obj), addr), daemon=True).start()
                                    # Avoid spamming main log for media chunks
                                    if t != "media":
                                        self.logger.info(
                                            "Control forwarded immediately: t=%s stream=%s from id=%s to %d recipients",
                                            t, info.stream_id, sender_logical_id, recipients_now,
                                        )
                                except OSError as e:
                                    # If send fails, log and keep running
                                    self.logger.debug("send control error: %s", e)
                                except Exception:
                                    pass
                            elif t in ("media_end", "media_req"):
                                # For late joiners to trigger missing-chunk requests, include media_end in history
                                if t == "media_end":
                                    try:
                                        mid_val = obj.get("mid")
                                        if isinstance(mid_val, str) and mid_val:
                                            seen = self._history_seen_media_mids.setdefault(info.stream_id, set())
                                            # Skip duplicate media_end for the same mid to avoid multiple replays
                                            if mid_val not in seen:
                                                # Ensure we always include basic fields so receiver can create accumulator
                                                base = {
                                                    "t": "media_end",
                                                    "mid": mid_val,
                                                    "total": obj.get("total"),
                                                    "mtype": obj.get("mtype") or "image",
                                                    "mime": obj.get("mime") or "image/jpeg",
                                                    # include cr (chunkRaw) so late joiners know chunk size
                                                    "cr": obj.get("cr") or 900,
                                                }
                                                s = json.dumps(base, ensure_ascii=False)
                                                lst = self.chat_history.setdefault(info.stream_id, [])
                                                lst.append(s)
                                                if len(lst) > 2000:
                                                    del lst[: len(lst) - 2000]
                                                seen.add(mid_val)
                                    except Exception:
                                        pass
                                # Forward media control signals as well so receivers can request retransmit
                                try:
                                    recipients_now, recipients_addrs = self._send_control_to_stream_excluding(info.stream_id, obj, addr, with_addrs=True)
                                    self.logger.info(
                                        "Control forwarded immediately: t=%s stream=%s from id=%s to %d recipients",
                                        t, info.stream_id, sender_logical_id, recipients_now,
                                    )
                                    self.detail_logger.debug("Forwarded t=%s to addrs=%s", t, recipients_addrs)
                                except OSError as e:
                                    self.logger.debug("send media control error: %s", e)
                                except Exception:
                                    pass
                            elif t in ("chat_delivered", "chat_read", "media_delivered", "media_read"):
                                # Forward these status updates only to the original sender by logical id
                                try:
                                    orig_id = None
                                    if obj.get("orig_id") is not None:
                                        orig_id = int(obj.get("orig_id"))
                                except Exception:
                                    orig_id = None
                                if orig_id is not None:
                                    target_addr = None
                                    for a, c in list(self.clients.items()):
                                        if c.stream_id == info.stream_id and c.client_id == orig_id:
                                            target_addr = a
                                            break
                                    if target_addr is not None:
                                        try:
                                            payload = b"\x01" + json.dumps(obj, ensure_ascii=False).encode("utf-8")
                                            data_uc = AudioPacket.encode(AudioPacket(stream_id=info.stream_id, sequence=0, timestamp_ms=0, codec=0, payload=payload))
                                            self.sock.sendto(data_uc, target_addr)
                                        except Exception:
                                            pass

                else:
                    # If this looked like control but failed to parse, log a hint
                    if packet.payload and packet.payload[0] == 1:
                        try:
                            snippet = packet.payload[1:1+120].decode("utf-8", errors="ignore")
                        except Exception:
                            snippet = "<decode_error>"
                        self.logger.info("Malformed control JSON from %s: %s", addr, snippet)

                # Relay raw packet to all in same stream (exclude sender). For control we already forwarded above,
                # но оставим raw-ретрансляцию только для аудио, чтобы не дублировать control у клиентов.
                recipients = 0
                for other_addr, other in list(self.clients.items()):
                    if other_addr == addr or other.stream_id != packet.stream_id:
                        continue
                    try:
                        # Если это control, raw уже разослали — пропустим raw-дублирование
                        if control_type is None and packet.payload and packet.payload[0] == 0:
                            self.sock.sendto(data, other_addr)
                            recipients += 1
                        else:
                            # control: пропускаем raw
                            pass
                    except OSError as e:
                        self.logger.warning("Failed to relay to %s: %s", other_addr, e)
                # Log control relay statistics (skip media chunks to avoid flooding)
                if control_type == "chat":
                    self.logger.info(
                        "Relayed chat: stream=%s from id=%s to %d recipients",
                        packet.stream_id, sender_logical_id, recipients,
                    )
                if control_type in ("media_end", "media_req"):
                    self.detail_logger.debug("Relayed %s: stream=%s from id=%s to %d recipients", control_type, packet.stream_id, sender_logical_id, recipients)
            except Exception as e:
                # Prevent unexpected exceptions from stopping the server loop
                try:
                    self.logger.exception("Unhandled error while processing packet from %s: %s", addr, e)
                except Exception:
                    pass
                # keep loop alive
                continue

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
            try:
                self.detail_logger.debug("Presence unicast to %s for stream=%s", addr, stream_id)
            except Exception:
                pass
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
        addrs: list[Tuple[str, int]] = []
        for addr, c in list(self.clients.items()):
            if c.stream_id != stream_id:
                continue
            try:
                self.sock.sendto(data, addr)
                addrs.append(addr)
            except OSError:
                pass
        try:
            self.detail_logger.debug("Broadcast control t=%s to addrs=%s", obj.get("t"), addrs)
        except Exception:
            pass

    def _send_control_to_stream_excluding(self, stream_id: int, obj: dict, exclude_addr: Tuple[str, int], with_addrs: bool = False):
        """Broadcast control to all in stream except exclude_addr.
        Returns recipients count, and optionally list of addresses if with_addrs=True.
        """
        payload = b"\x01" + json.dumps(obj, ensure_ascii=False).encode("utf-8")
        pkt = AudioPacket(stream_id=stream_id, sequence=0, timestamp_ms=0, codec=0, payload=payload)
        data = AudioPacket.encode(pkt)
        recipients = 0
        addrs: list[Tuple[str, int]] = []
        for addr, c in list(self.clients.items()):
            if c.stream_id != stream_id or addr == exclude_addr:
                continue
            try:
                self.sock.sendto(data, addr)
                recipients += 1
                addrs.append(addr)
            except OSError:
                pass
        return (recipients, addrs) if with_addrs else recipients

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


