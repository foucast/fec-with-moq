"""Source-block <-> FEC-symbol mapping.

The one piece of arithmetic the design needs and MoQT does not provide: a
deterministic map from source objects to symbol indices, so a lost source object
becomes a KNOWN set of erased symbols at the receiver.

Two layouts, because the choice is a real tradeoff and milestone 2a measures it:

  SPAN   objects packed back-to-back into one byte stream, cut into symbols.
         Only the block's last symbol is padded. But a symbol straddling two
         objects is erased if EITHER is lost -- the receiver holds the surviving
         neighbour's bytes, yet an erasure code treats a symbol as all-or-nothing.

  PAD    each object padded up to a whole number of symbols. Erasures align
         exactly with objects, at the cost of padding every object.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from math import ceil

# 1200 was wrong -- corrected 2026-09-27 against aiomoqt's loopback_bench:
# ObjectDatagrams cap around 1150B payload (aiomoqt -D help text: "object must
# fit one packet, ~1150B max payload"). Left headroom below that ceiling for
# MoQT/QUIC datagram framing overhead. [open] verify against a real path MTU.
# FECMOQ_SYMBOL_SIZE overrides it for an experiment (2026-10-07, datagram-size test through a
# third-party relay); every role and the analysis must then use the same value (run_demo passes
# it to all three). Not a default change.
SYMBOL_SIZE = int(os.environ.get("FECMOQ_SYMBOL_SIZE", "1100"))

SPAN = "span"
PAD = "pad"


@dataclass(frozen=True)
class BlockLayout:
    mode: str
    symbol_size: int
    object_sizes: tuple[int, ...]
    object_starts: tuple[int, ...]   # byte offset of each object in the padded stream
    k: int                           # number of source symbols

    @property
    def payload_bytes(self) -> int:
        return sum(self.object_sizes)

    @property
    def padding_bytes(self) -> int:
        return self.k * self.symbol_size - self.payload_bytes


def layout_block(object_sizes, mode: str = SPAN, symbol_size: int = SYMBOL_SIZE) -> BlockLayout:
    sizes = tuple(int(s) for s in object_sizes)
    if any(s <= 0 for s in sizes):
        raise ValueError("object sizes must be positive")
    starts, off = [], 0
    for s in sizes:
        starts.append(off)
        off += ceil(s / symbol_size) * symbol_size if mode == PAD else s
    if mode not in (SPAN, PAD):
        raise ValueError(f"unknown mode {mode!r}")
    return BlockLayout(mode, symbol_size, sizes, tuple(starts), ceil(off / symbol_size))


def symbols_for_object(layout: BlockLayout, idx: int) -> range:
    """Symbol indices an object touches (including partially-shared boundary symbols)."""
    start = layout.object_starts[idx]
    end = start + layout.object_sizes[idx]          # exclusive
    s = layout.symbol_size
    return range(start // s, (end - 1) // s + 1)


def erased_symbols(layout: BlockLayout, missing) -> set[int]:
    """Translate missing source objects into the source-symbol indices they erase."""
    out: set[int] = set()
    for i in missing:
        out.update(symbols_for_object(layout, i))
    return out


def to_symbols(layout: BlockLayout, objects) -> list[bytes]:
    buf = bytearray(layout.k * layout.symbol_size)
    for start, obj in zip(layout.object_starts, objects):
        buf[start:start + len(obj)] = obj
    s = layout.symbol_size
    return [bytes(buf[i * s:(i + 1) * s]) for i in range(layout.k)]


def from_symbols(layout: BlockLayout, symbols) -> list[bytes]:
    buf = b"".join(symbols)
    return [buf[st:st + n] for st, n in zip(layout.object_starts, layout.object_sizes)]
