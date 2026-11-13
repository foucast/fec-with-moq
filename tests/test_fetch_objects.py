"""Repair by FETCH, the publisher's side (fecmoq.moq.fetch_objects, served by relay_fec.py pub
--serve-fetch): a standalone FETCH of the source track or of a rung returns exactly the Objects the
track's own publisher sends for that range, from blocks already produced.

The range is as FETCH names it: Start Location included, End Object one past the last one wanted,
End Object 0 = the whole End Group. The multi-block group test needs FECMOQ_TEST_MP4_GOP (an mp4
with GOP FECMOQ_TEST_GOP, default 120) and ISA-L; it is skipped otherwise.
"""
import os

import pytest

from fecmoq.codec import Coder
from fecmoq.moq import OBJECT_STRIDE, BlockSchedule, fetch_objects
from fecmoq.symbols import SPAN, layout_block, to_symbols
from fecmoq.wire import pack_repair, unpack_repair


@pytest.fixture
def sch(monkeypatch):
    monkeypatch.setenv("FECMOQ_CODER", "isal")
    monkeypatch.delenv("FECMOQ_ENCODE", raising=False)
    s = BlockSchedule(seed=7, n_blocks=6, n_objects=12, gap_s=0.2, rung_fracs=(0.15, 0.15, 0.15))
    s.last_out = 3          # blocks 0..3 produced, 4 and 5 not yet
    return s


def test_rung_serves_its_own_symbols(sch):
    for rung in (1, 2, 3):
        sizes, slices, repair = sch.repair(2)
        got = fetch_objects(sch, rung, 2, 0, 2, 0)
        # one block per group: group = block, object = global repair index
        assert [(g, o) for g, o, _ in got] == [(2, j) for j in slices[rung - 1]]
        assert [p for _, _, p in got] == [pack_repair(sizes, repair[j]) for j in slices[rung - 1]]


def test_source_serves_the_symbols_the_track_sends(sch):
    blk = sch.block(1)
    sizes = [len(x) for x in blk]
    syms = to_symbols(layout_block(sizes, SPAN), blk)
    got = fetch_objects(sch, None, 1, 2, 1, 5)       # objects 2, 3, 4
    assert [(g, o) for g, o, _ in got] == [(1, 2), (1, 3), (1, 4)]
    for g, o, p in got:
        assert p == pack_repair(sizes, syms[o])


def test_end_object_is_exclusive_and_zero_is_the_whole_group(sch):
    k = layout_block([len(x) for x in sch.block(0)], SPAN).k
    assert len(fetch_objects(sch, None, 0, 0, 0, 0)) == k
    assert [o for _, o, _ in fetch_objects(sch, None, 0, 0, 0, 1)] == [0]
    # across groups: from 0.3 to the end of group 1
    got = fetch_objects(sch, None, 0, 3, 1, 0)
    k1 = layout_block([len(x) for x in sch.block(1)], SPAN).k
    assert [(g, o) for g, o, _ in got] == [(0, o) for o in range(3, k)] + [(1, o) for o in range(k1)]


def test_blocks_not_yet_produced_are_not_served(sch):
    assert fetch_objects(sch, 1, 4, 0, 5, 0) == []
    assert fetch_objects(sch, None, 3, 0, 4, 0) == fetch_objects(sch, None, 3, 0, 3, 0)


def test_a_range_outside_the_rung_is_empty(sch):
    _, slices, _ = sch.repair(2)
    hi = slices[2].stop
    assert fetch_objects(sch, 1, 2, slices[0].stop, 2, hi) == []


MP4_GOP = os.environ.get("FECMOQ_TEST_MP4_GOP", "")
GOP = int(os.environ.get("FECMOQ_TEST_GOP", "120"))


@pytest.mark.skipif(not os.path.exists(MP4_GOP), reason="set FECMOQ_TEST_MP4_GOP to a GOP mp4")
def test_blocks_within_a_group_by_object_stride():
    try:
        Coder(2, 1, "isal")
    except RuntimeError:
        pytest.skip("ISA-L not available")
    from fecmoq.media import MediaBlockSchedule
    s = MediaBlockSchedule(media=MP4_GOP, gop_frames=GOP, seed=7, n_blocks=2 * GOP // 12,
                           n_objects=12, gap_s=0.2)
    per = s.blocks_per_group
    s.last_out = per + 3
    # group 1, its block 2 (block per + 2): source symbols at 2 * stride + j
    b = per + 2
    k = layout_block([len(x) for x in s.block(b)], SPAN).k
    got = fetch_objects(s, None, 1, 2 * OBJECT_STRIDE, 1, 2 * OBJECT_STRIDE + k)
    assert [o for _, o, _ in got] == [2 * OBJECT_STRIDE + j for j in range(k)]
    # rung 2 of the same block: the rung's global repair indices, same base
    _, slices, _ = s.repair(b)
    got = fetch_objects(s, 2, 1, 2 * OBJECT_STRIDE, 1, 3 * OBJECT_STRIDE)
    assert [o for _, o, _ in got] == [2 * OBJECT_STRIDE + j for j in slices[1]]
    # a block of the group not produced yet is not served
    assert fetch_objects(s, None, 1, 4 * OBJECT_STRIDE, 1, 5 * OBJECT_STRIDE) == []
