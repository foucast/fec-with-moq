"""Shaka phase 2, stage B check: the FEC publisher's real-media blocks survive the symbol path.

Real LOC frames from an mp4 (MediaBlockSchedule), cut into symbols and packed exactly as the arm-B
datagram source track sends them, erased in various patterns, decoded with ISA-L, cut back into
objects: the frames must come back byte for byte, or decoding must fail cleanly when there are more
erasures than repair symbols.

    FECMOQ_TEST_MP4=clip.mp4 FECMOQ_CODER=isal pytest tests/test_media_fec.py

Needs an mp4 whose GOP is 12 frames (keyint=12) and ISA-L; skipped otherwise. The multi-block
group tests also need FECMOQ_TEST_MP4_GOP, an mp4 with GOP FECMOQ_TEST_GOP (default 120).
"""
import os
import random

import pytest

from fecmoq.codec import Coder
from fecmoq.media import LOC_PROP_TIMESTAMP, MediaBlockSchedule, unpack_loc_object
from fecmoq.moq import rung_slices
from fecmoq.symbols import SPAN, erased_symbols, from_symbols, layout_block, to_symbols
from fecmoq.wire import pack_repair, unpack_repair

MP4 = os.environ.get("FECMOQ_TEST_MP4", "")
pytestmark = pytest.mark.skipif(not os.path.exists(MP4), reason="set FECMOQ_TEST_MP4 to a GOP-12 mp4")

N = 12
BLOCKS = 40


@pytest.fixture(scope="module")
def schedule():
    try:
        Coder(2, 1, "isal")
    except RuntimeError:
        pytest.skip("ISA-L not available")
    return MediaBlockSchedule(media=MP4, seed=7, n_blocks=BLOCKS, n_objects=N, gap_s=0.2)


def _decode_props(props: bytes) -> dict:
    from aiomoqt.messages.base import MOQTMessage
    from aiomoqt.utils.buffer import Buffer
    buf = Buffer(data=props, vi64=True)
    return MOQTMessage._extensions_decode(buf, with_length=False, buf_end=len(props), delta=True)


def _wire_block(objs):
    """Source symbols and repair symbols as they go on the wire (arm B, rungs 0.15 x 3)."""
    sizes = [len(o) for o in objs]
    lay = layout_block(sizes, SPAN)
    syms = to_symbols(lay, objs)
    m = rung_slices(lay.k, [0.15] * 3)[-1].stop
    rep = Coder(lay.k, m, "isal").encode(syms)
    src_wire = [pack_repair(sizes, s, "isal") for s in syms]
    rep_wire = [pack_repair(sizes, r, "isal") for r in rep]
    return lay, m, src_wire, rep_wire


def _receive(lay, src_wire, rep_wire, missing, reps):
    """What a receiver rebuilds from the datagrams that arrived: None if undecodable."""
    k = lay.k
    avail = {j: unpack_repair(src_wire[j], expect="isal")[1] for j in range(k) if j not in missing}
    avail.update({k + j: unpack_repair(rep_wire[j], expect="isal")[1] for j in reps})
    syms = Coder(k, max(1, len(rep_wire)), "isal").decode(avail)
    return None if syms is None else from_symbols(lay, syms)


def test_blocks_are_the_mp4_gops_with_ordered_timestamps(schedule):
    last = None
    for b in range(BLOCKS):
        objs = schedule.block(b)
        assert len(objs) == N
        for i, obj in enumerate(objs):
            props, payload = unpack_loc_object(obj)
            assert payload == schedule.media.block(b)[i][0]
            ts = _decode_props(props)[LOC_PROP_TIMESTAMP]
            assert last is None or ts > last
            last = ts
        assert schedule.media.block(b)[0][1], "every block starts on a keyframe"


def test_blocks_are_memoised(schedule):
    assert schedule.block(3) is schedule.block(3)


def test_stamps_start_when_production_starts(monkeypatch):
    """t0 is the wall clock at the first slot() (production starts once the gate is open), not when
    block 0 was first built: relay_fec.py builds block 0 at startup to size the FETCH window, then
    waits for subscribers."""
    import asyncio

    from fecmoq import media
    s = MediaBlockSchedule(media=MP4, seed=7, n_blocks=BLOCKS, n_objects=N, gap_s=0.2)

    def stamps(b):
        return [_decode_props(unpack_loc_object(o)[0])[LOC_PROP_TIMESTAMP] for o in s.block(b)]

    monkeypatch.setattr(media.time, "time", lambda: 1000.0)
    early = stamps(0)                                       # at startup: provisional
    monkeypatch.setattr(media.time, "time", lambda: 1030.0)  # the gate opens 30 s later
    asyncio.run(s.slot(0))
    frame_us = 0.2e6 / N
    assert early[0] == 1000 * 10**6
    assert stamps(0) == [1030 * 10**6 + int(i * frame_us) for i in range(N)]
    assert stamps(1)[0] == 1030 * 10**6 + 200_000
    assert s.block(0) is s.block(0)


def test_frames_rebuild_byte_identical_under_erasure(schedule):
    rng = random.Random(10)
    checked = 0
    for b in range(BLOCKS):
        objs = schedule.block(b)
        lay, m, src_wire, rep_wire = _wire_block(objs)
        k = lay.k
        r1 = rung_slices(k, [0.15] * 3)[0]
        patterns = [
            ("random", set(rng.sample(range(k), rng.randint(1, m))), list(range(m))),
            ("max erasure", set(rng.sample(range(k), m)), list(range(m))),
            ("keyframe lost, all rungs", set(erased_symbols(lay, [0])), list(range(m))),
            ("one frame lost, rung 1", set(erased_symbols(lay, [rng.randrange(1, N)])), list(r1)),
        ]
        for name, missing, reps in patterns:
            got = _receive(lay, src_wire, rep_wire, missing, reps)
            if len(missing) <= len(reps):
                assert got is not None, (b, name)
                assert got == objs, (b, name)
                for i, o in enumerate(got):
                    assert unpack_loc_object(o)[1] == schedule.media.block(b)[i][0]
                checked += 1
            else:
                assert got is None, (b, name)
        too_many = set(rng.sample(range(k), min(k, m + 1)))
        if len(too_many) > m:
            assert _receive(lay, src_wire, rep_wire, too_many, list(range(m))) is None
    assert checked >= 2 * BLOCKS


# Several blocks per group (stage 3): a 2 s GOP (keyint=120) cut into 12-frame blocks.
MP4_GOP = os.environ.get("FECMOQ_TEST_MP4_GOP", "")
GOP = int(os.environ.get("FECMOQ_TEST_GOP", "120"))


@pytest.fixture(scope="module")
def gop_schedule():
    if not os.path.exists(MP4_GOP):
        pytest.skip(f"set FECMOQ_TEST_MP4_GOP to a GOP-{GOP} mp4")
    try:
        Coder(2, 1, "isal")
    except RuntimeError:
        pytest.skip("ISA-L not available")
    return MediaBlockSchedule(media=MP4_GOP, gop_frames=GOP, seed=7, n_blocks=3 * GOP // N,
                              n_objects=N, gap_s=0.2)


def test_gop_blocks_and_addresses(gop_schedule):
    from fecmoq.moq import OBJECT_STRIDE
    per = GOP // N
    assert gop_schedule.blocks_per_group == per
    for b in range(3 * per):
        assert gop_schedule.media.block(b)[0][1] == (b % per == 0), b
        assert len(gop_schedule.block(b)) == N
    assert gop_schedule.address(0, 0) == (0, 0, True)
    assert gop_schedule.address(per - 1, 5) == (0, (per - 1) * OBJECT_STRIDE + 5, False)
    assert gop_schedule.address(per + 2, 0) == (1, 2 * OBJECT_STRIDE, False)
    assert gop_schedule.address(2 * per, 0) == (2, 0, True)
    # Every rung starts each group (its first symbol of the group's first block), whatever its
    # global repair indices: rungs 2 and 3, whose indices don't include 0, once never did, so
    # their datagram lanes never opened and they delivered nothing.
    for b in range(3 * per):
        sizes, slices, _ = gop_schedule.repair(b)
        assert len(slices) == 3
        for mine in slices:
            starts = [gop_schedule.repair_address(b, n, j)[2] for n, j in enumerate(mine)]
            assert starts == [b % per == 0 and n == 0 for n in range(len(mine))], (b, mine)
            assert [gop_schedule.repair_address(b, n, j)[:2] for n, j in enumerate(mine)] == \
                [gop_schedule.address(b, j)[:2] for j in mine]


def test_gop_blocks_rebuild_byte_identical(gop_schedule):
    rng = random.Random(11)
    for b in range(3 * GOP // N):
        objs = gop_schedule.block(b)
        lay, m, src_wire, rep_wire = _wire_block(objs)
        missing = set(rng.sample(range(lay.k), rng.randint(1, m)))
        got = _receive(lay, src_wire, rep_wire, missing, list(range(m)))
        assert got == objs, b
        for i, o in enumerate(got):
            assert unpack_loc_object(o)[1] == gop_schedule.media.block(b)[i][0]
