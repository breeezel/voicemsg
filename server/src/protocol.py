from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import struct


MAGIC = 0xA11D
VERSION = 1


@dataclass(slots=True)
class AudioPacket:
    """
    Simple UDP packet for raw PCM frames.
    Header format (network byte order):
      - magic:   2 bytes  (0xA11D)
      - version: 1 byte   (1)
      - stream:  2 bytes  (uint16) logical stream id
      - seq:     4 bytes  (uint32) sequence number
      - ts_ms:   4 bytes  (uint32) timestamp in ms (client clock)
      - codec:   1 byte   (0=PCM16, 1=OPUS)
    Total header: 14 bytes
    """

    stream_id: int
    sequence: int
    timestamp_ms: int
    codec: int
    payload: bytes

    HEADER_STRUCT = struct.Struct(
        ">HBHIIB"  # magic(2), version(1), stream(2), seq(4), ts_ms(4), codec(1)
    )

    @classmethod
    def encode(cls, packet: "AudioPacket") -> bytes:
        magic16 = MAGIC & 0xFFFF
        header = cls.HEADER_STRUCT.pack(
            magic16, VERSION, packet.stream_id & 0xFFFF, packet.sequence & 0xFFFFFFFF,
            packet.timestamp_ms & 0xFFFFFFFF, packet.codec & 0xFF
        )
        return header + packet.payload

    @classmethod
    def decode(cls, data: bytes) -> "AudioPacket | None":
        if len(data) < cls.HEADER_STRUCT.size:
            return None
        magic16, version, stream_id, seq, ts_ms, codec = cls.HEADER_STRUCT.unpack(
            data[: cls.HEADER_STRUCT.size]
        )
        if magic16 != (MAGIC & 0xFFFF) or version != VERSION:
            return None
        return AudioPacket(
            stream_id=stream_id,
            sequence=seq,
            timestamp_ms=ts_ms,
            codec=codec,
            payload=data[cls.HEADER_STRUCT.size :],
        )


PCM16_CODEC = 0
OPUS_CODEC = 1


def default_recording_dir() -> Path:
    base = Path.home() / "voicemsg_recordings"
    base.mkdir(parents=True, exist_ok=True)
    return base


