"""Repair-object wire format -- streaming-format level, no MoQT extension headers.

Every repair object carries the block's object sizes, so the receiver can rebuild
the symbol layout from ANY single repair symbol even when the source objects that
would reveal those sizes are exactly the ones that were lost.

  codec:u8 | n_objects:u8 | size[0]:u16 ... size[n-1]:u16 | symbol

The first byte was a format version; it now also records which coder produced the
symbol: 1 = zfec (byte-identical to every object sent before backends existed),
2 = ISA-L. Its high bit (0x80) marks the INDEXED form, which carries the symbol's global
repair index as u16 after the sizes:

  codec|0x80:u8 | n_objects:u8 | size[0]:u16 ... size[n-1]:u16 | index:u16 | symbol

used when a rung numbers its objects 0..n-1 per group instead of by global repair index
(--repair-ids per-rung). A relay cache treats a group whose first object id isn't 0 as
missing objects 0..first-1 (moqx asks upstream for them and abandons the whole FETCH when
that fails), so per-rung numbering makes every rung fetchable. 12 objects -> 28B + 1100B. The backends produce different repair bytes, so a receiver must decode
with the one that encoded; unpack_repair refuses a mismatch instead of returning
garbage.

Deliberately not in MoQT object extension headers: keeping it in the payload uses
only the most basic wire features, which is the strongest form of the
"no protocol change" claim. 12 objects -> 26B header + 1100B symbol = 1126B,
under aiomoqt's 1152B DATAGRAM payload ceiling.
"""
from __future__ import annotations

import struct

VERSION = 1                       # kept: zfec objects are unchanged on the wire
CODEC_IDS = {"zfec": 1, "isal": 2}
CODEC_NAMES = {v: k for k, v in CODEC_IDS.items()}
DATAGRAM_MAX_PAYLOAD = 1152   # aiomoqt loopback_bench: "max ~1152B payload at 1200B frame ceiling"


INDEXED = 0x80


def pack_repair(object_sizes, symbol: bytes, codec: str | None = None,
                index: int | None = None) -> bytes:
    """index: the global repair index, carried in the payload (INDEXED form) when the
    object id doesn't carry it."""
    from .codec import default_backend
    codec_id = CODEC_IDS[(codec or default_backend())]
    n = len(object_sizes)
    if n > 255 or any(s > 0xFFFF for s in object_sizes):
        raise ValueError("header supports <=255 objects of <=65535B each")
    if index is None:
        hdr = struct.pack(f"!BB{n}H", codec_id, n, *object_sizes)
    else:
        hdr = struct.pack(f"!BB{n}HH", codec_id | INDEXED, n, *object_sizes, index)
    out = hdr + symbol
    if len(out) > DATAGRAM_MAX_PAYLOAD:
        raise ValueError(f"repair object {len(out)}B exceeds {DATAGRAM_MAX_PAYLOAD}B datagram ceiling")
    return out


def repair_codec(payload: bytes) -> str:
    """Which coder produced this repair object."""
    cid = payload[0] & ~INDEXED
    if cid not in CODEC_NAMES:
        raise ValueError(f"unknown repair codec id {cid}")
    return CODEC_NAMES[cid]


def unpack_repair(payload: bytes, expect: str | None = None) -> tuple[list[int], bytes]:
    """expect: the coder this receiver will decode with (default: FECMOQ_CODER, else isal)."""
    from .codec import default_backend
    codec = repair_codec(payload)
    want = expect or default_backend()
    if codec != want:
        raise ValueError(f"repair object encoded with {codec!r} but this receiver decodes "
                         f"with {want!r}: set FECMOQ_CODER={codec} on every node")
    cid, n = struct.unpack_from("!BB", payload)
    sizes = list(struct.unpack_from(f"!{n}H", payload, 2))
    return sizes, payload[2 + 2 * n + (2 if cid & INDEXED else 0):]


def repair_index(payload: bytes) -> int | None:
    """The global repair index an INDEXED repair object carries, else None (the object
    id is the index)."""
    cid, n = struct.unpack_from("!BB", payload)
    if not cid & INDEXED:
        return None
    return struct.unpack_from("!H", payload, 2 + 2 * n)[0]
