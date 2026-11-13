"""Real media as FEC source objects (Shaka phase 2, option 1).

The source block is a GOP of LOC video frames from an mp4 (twelve per block, keyframe first), each
serialized as one source object:

    props_len:u16 | LOC properties | frame payload

The properties are the bytes a LOC object would carry in its MoQ object-properties block, already in
the draft-18 encoding (delta-coded types, vi64), because they ride opaquely inside our symbols and
the browser's LOC parser reads them with its draft-18 codec whatever draft the publisher speaks to
the relay. AV1 needs only the timestamp property (0x10): its config OBUs travel in-band.

MediaBlockSchedule replaces BlockSchedule.block(b), so every arm-B publisher path (datagram source
symbols, repair rungs, spread emission, incremental ISA-L) runs unchanged on real frames.
"""
from __future__ import annotations

import json
import struct
import time

from .moq import BlockSchedule

LOC_PROP_TIMESTAMP = 0x10        # vi64, µs since the Unix epoch (no TIMESCALE property)
MAX_OBJECT = 0xFFFF              # the repair header carries sizes as u16


def encode_properties_d18(props: dict) -> bytes:
    """A MoQ properties block (no length) in the draft-18 encoding, via aiomoqt's own encoder."""
    from aiomoqt.messages.base import MOQTMessage
    from aiomoqt.utils.buffer import Buffer
    buf = Buffer(capacity=256, vi64=True)
    MOQTMessage._extensions_encode(buf, props, with_length=False, delta=True)
    return bytes(buf.data[:buf.tell()])


def pack_loc_object(props: bytes, payload: bytes) -> bytes:
    out = struct.pack("!H", len(props)) + props + payload
    if len(out) > MAX_OBJECT:
        raise ValueError(f"LOC object {len(out)} B exceeds {MAX_OBJECT} B (u16 sizes in the repair header)")
    return out


def unpack_loc_object(obj: bytes) -> tuple[bytes, bytes]:
    (n,) = struct.unpack_from("!H", obj)
    return obj[2:2 + n], obj[2 + n:]


class Mp4Gops:
    """The video track of an mp4 cut into blocks of `n` frames, `gop` frames to a GOP (default n:
    a block is a GOP). `gop` must be a multiple of n and every GOP must start on a keyframe (encode
    with keyint = gop), so a GOP is exactly `blocks_per_gop` blocks and only a GOP's first block
    starts with a keyframe."""

    def __init__(self, path: str, n: int, gop: int | None = None):
        from aiomoqt.media.sources import Mp4Reader
        gop = gop or n
        if gop % n:
            raise ValueError(f"GOP of {gop} frames is not a whole number of {n}-frame blocks")
        v = Mp4Reader(path).video
        if v is None:
            raise ValueError(f"{path}: no video track")
        samples = [(s.payload, s.key_frame) for s in v.samples()]
        if len(samples) < gop:
            raise ValueError(f"{path}: {len(samples)} frames, fewer than one GOP of {gop}")
        whole = len(samples) // gop * gop
        bad = [i // gop for i in range(0, whole, gop) if not samples[i][1]]
        if bad:
            raise ValueError(f"{path}: GOP {bad[0]} doesn't start on a keyframe; encode with keyint={gop}")
        self.blocks = [samples[i:i + n] for i in range(0, whole, n)]
        self.blocks_per_gop = gop // n
        self.codec, self.width, self.height, self.fps = v.codec_string, v.width, v.height, v.fps
        self.largest = max(len(p) for p, _ in samples)

    def block(self, i: int) -> list[tuple[bytes, bool]]:
        return self.blocks[i % len(self.blocks)]


class MediaBlockSchedule(BlockSchedule):
    """BlockSchedule whose block b is block b of the (looping) mp4 as serialized LOC objects: GOP b
    when gop_frames is the block size (the default), else one of the GOP's blocks_per_group blocks.
    Frame i of block b is stamped t0 + b * gap + i * gap / n (µs), so the timeline runs at the block
    rate. t0 is the wall clock when production starts (the first slot(), once the gate is open), so
    the stamps don't carry the wait for subscribers. A block built before that (relay_fec.py sizes
    the FETCH window from block 0 at startup) has provisional stamps and is dropped at the start;
    nothing sends it. Blocks are memoised: the source track and the encoder must see the same
    bytes."""

    def __init__(self, *, media: str, gop_frames: int | None = None, **kw):
        super().__init__(**kw)
        self.media = Mp4Gops(media, self.n_objects, gop_frames)
        self.blocks_per_group = self.media.blocks_per_gop
        self._t0_us = None
        self._started = False
        self._memo: dict[int, list[bytes]] = {}

    async def slot(self, b: int) -> None:
        if not self._started:
            self._started = True
            self._t0_us = int(time.time() * 1e6) - int(b * self.gap_s * 1e6)
            self._memo.clear()          # built before the start: provisional stamps, never sent
        await super().slot(b)

    def block(self, b: int) -> list[bytes]:
        if b in self._memo:
            return self._memo[b]
        if self._t0_us is None:
            self._t0_us = int(time.time() * 1e6) - int(b * self.gap_s * 1e6)
        gap_us, frame_us = self.gap_s * 1e6, self.gap_s * 1e6 / self.n_objects
        objs = []
        for i, (payload, _key) in enumerate(self.media.block(b)):
            ts = self._t0_us + int(b * gap_us + i * frame_us)
            objs.append(pack_loc_object(encode_properties_d18({LOC_PROP_TIMESTAMP: ts}), payload))
        self._memo[b] = objs
        for old in [k for k in self._memo if k < b - self.KEEP - 2]:
            del self._memo[old]
        return objs


def catalog_json(media: Mp4Gops, *, n_objects: int, gap_s: float, rungs: list[float],
                 symbol_size: int, source: str = "src", object_stride: int = 256) -> bytes:
    """The MSF catalog: the source track as LOC video Shaka can configure a decoder from, and the
    repair rungs under an unregistered packaging so a player without the FEC stage skips them."""
    from aiomoqt.media.catalog import Catalog, CatalogTrack
    rung_names = [f"rep{r}" for r in range(1, len(rungs) + 1)]
    fec = {"scheme": "rs-cauchy-gf256", "codec": 2, "symbolSize": symbol_size,
           "blockObjects": n_objects, "blockSeconds": gap_s, "rungs": rung_names,
           "rungFractions": list(rungs), "objectFormat": "props_len:u16|properties|payload",
           # A MoQ group is a GOP of blocksPerGroup blocks; symbol j of the group's q-th block is
           # Object q * objectStride + j (source and repair alike).
           "blocksPerGroup": media.blocks_per_gop, "objectStride": object_stride}
    tracks = [CatalogTrack(name=source, packaging="loc", isLive=True, role="video", renderGroup=1,
                           codec=media.codec, width=media.width, height=media.height,
                           framerate=round(n_objects / gap_s, 3), extra={"x-fec": fec})]
    for r, name in enumerate(rung_names, 1):
        tracks.append(CatalogTrack(name=name, packaging="x-fec-repair", isLive=True,
                                   extra={"x-fec": {"source": source, "rung": r}}))
    cat = Catalog(generatedAt=int(time.time() * 1000), tracks=tracks)
    return cat.to_json().encode()


def catalog_dict(blob: bytes) -> dict:
    return json.loads(blob)
