"""FETCH answered by the publisher (relay_fec.py pub --serve-fetch, the default): fecmoq.moq.
track_objects must repeat exactly what each live track writes -- group and object ids, payloads,
END_OF_GROUP status objects -- for every source mapping and repair option, and ScheduleHistory must
answer a range reaching past its window as aiomoqt's ObjectHistory does: FETCH objects for what it
holds, one End of Unknown Range marker for the rest.

The live side is captured by running each track's own produce() into a recording `out`.
"""
import asyncio
from types import SimpleNamespace

import pytest

import fecmoq.moq as fmoq
from fecmoq.moq import (BlockSchedule, FetchSpec, ScheduleHistory, StreamMapping,
                        track_objects)

N_BLOCKS = 5


class Recorder:
    """produce()'s `out`: records every write, in order."""
    lanes = ()

    def __init__(self):
        self.writes = []

    async def write(self, group_id, object_id, payload, *, extensions=None, group_start=False):
        self.writes.append((group_id, object_id, payload, extensions))


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setenv("FECMOQ_CODER", "isal")
    monkeypatch.setenv("FECMOQ_REPAIR_AFTER", "queued")   # no aiopquic send buffer to wait on
    monkeypatch.delenv("FECMOQ_ENCODE", raising=False)


def schedule(bpg=1):
    s = BlockSchedule(seed=7, n_blocks=N_BLOCKS, n_objects=12, gap_s=0.01,
                      rung_fracs=(0.15, 0.15, 0.15))
    s.blocks_per_group = bpg
    return s


SESSION = SimpleNamespace()

SOURCES = {"group": fmoq.SourceTrack, "object": fmoq.ObjectSourceTrack,
           "datagram": fmoq.DatagramSourceTrack, "symbol-object": fmoq.SymbolStreamSourceTrack,
           "symbol-subgroup": fmoq.SymbolSubgroupSourceTrack}


def live(sch, src_cls, rep=None, **opts):
    """Run a source track and (optionally) one rung on one schedule; their writes."""
    common = dict(seed=7, n_blocks=N_BLOCKS, n_objects=12, gap_s=sch.gap_s, schedule=sch)
    src = src_cls(SESSION, **common)
    outs = [Recorder()]
    coros = [src.produce(outs[0])]
    if rep is not None:
        r = rep(SESSION, rung=opts.pop("rung"), rung_fracs=sch.rung_fracs, **common)
        for k, v in opts.items():
            setattr(r, k, v)
        outs.append(Recorder())
        # the rung first: one started after production begins joins at the newest block
        coros.insert(0, r.produce(outs[1]))

    async def run():
        await asyncio.gather(*coros)
    asyncio.run(run())
    return [o.writes for o in outs]


def fetched(sch, spec):
    return [r for b in range(N_BLOCKS) for r in track_objects(sch, spec, b)]


def data(recs):
    return sorted((g, o, p) for g, o, _, p, st in recs if st is None)


@pytest.mark.parametrize("mapping", list(SOURCES))
def test_source_fetch_repeats_the_live_track(mapping):
    sch = schedule()
    (writes,) = live(sch, SOURCES[mapping])
    got = fetched(sch, FetchSpec(kind="source", source_mapping=mapping))
    assert data(got) == sorted((g, o, p) for g, o, p, _ in writes)
    statuses = [(g, o) for g, o, _, _, st in got if st is not None]
    if mapping == "group":       # aiomoqt closes each group's stream with END_OF_GROUP at id n
        assert statuses == [(b, 12) for b in range(N_BLOCKS)]
    else:
        assert statuses == []


def test_datagram_source_several_blocks_per_group():
    sch = schedule(bpg=2)
    (writes,) = live(sch, fmoq.DatagramSourceTrack)
    got = fetched(sch, FetchSpec(kind="source", source_mapping="datagram"))
    assert data(got) == sorted((g, o, p) for g, o, p, _ in writes)
    assert {g for g, *_ in got} == {0, 1, 2}


@pytest.mark.parametrize("opts", [
    dict(),
    dict(per_rung_ids=True),
    dict(group_per_symbol=True),
    dict(end_of_group=True),
    dict(per_rung_ids=True, end_of_group=True),
])
@pytest.mark.parametrize("rung", [1, 2, 3])
def test_rung_fetch_repeats_the_live_track(rung, opts):
    sch = schedule()
    _, writes = live(sch, fmoq.DatagramSourceTrack, fmoq.RepairTrack, rung=rung, **opts)
    got = fetched(sch, FetchSpec(kind="repair", rung=rung, **opts))
    assert data(got) == sorted((g, o, p) for g, o, p, _ in writes)
    status = sorted((g, o) for g, o, _, _, st in got if st is not None)
    if opts.get("end_of_group"):
        # EogDatagramDelivery: the object written with EOG_MARK, then END_OF_GROUP at its id + 1
        marked = sorted((g, o + 1) for g, o, _, ext in writes if ext is fmoq.EOG_MARK)
        assert status == marked and len(marked) == N_BLOCKS
    else:
        assert status == []


def test_rung_fetch_several_blocks_per_group():
    sch = schedule(bpg=2)
    _, writes = live(sch, fmoq.DatagramSourceTrack, fmoq.RepairTrack, rung=2)
    got = fetched(sch, FetchSpec(kind="repair", rung=2))
    assert data(got) == sorted((g, o, p) for g, o, p, _ in writes)


def test_history_window_and_end_of_unknown_range():
    from aiomoqt.messages.data import FETCH_FLAGS_END_UNKNOWN
    from aiomoqt.types import ObjectStatus
    sch = schedule()
    sch.last_out = 3                                       # blocks 0..3 out, 4 not yet
    spec = FetchSpec(kind="source", source_mapping="group")
    h = ScheduleHistory(sch, spec, window_blocks=2)        # holds blocks 2 and 3
    assert h.largest() == (3, 11)
    assert h.evicted_through == (1, 12)                    # block 1's END_OF_GROUP
    got = h.fetch((0, 0), (3, 1 << 62))
    assert got[0].end_of_range == FETCH_FLAGS_END_UNKNOWN and (got[0].group_id, got[0].object_id) == (1, 12)
    body = got[1:]
    # the data objects only: a FETCH object has no Object Status from draft 16
    assert [(o.group_id, o.object_id) for o in body] == [(g, i) for g in (2, 3) for i in range(12)]
    assert all(o.status == ObjectStatus.NORMAL for o in body)
    assert h.last == (24, 1, (1, 13), (3, 1 << 62))     # served from just past the marker
    # inside the window: no marker
    got = h.fetch((3, 0), (3, 1 << 62))
    assert all(o.end_of_range is None for o in got) and len(got) == 12
    # descending: groups high to low, the marker last
    got = h.fetch((0, 0), (3, 1 << 62), descending=True)
    assert [o.group_id for o in got[:-1]][::12] == [3, 2] and got[-1].end_of_range is not None


def test_history_unsubscribed_rung_and_group_per_symbol():
    sch = schedule()
    sch.last_out = 4
    h = ScheduleHistory(sch, FetchSpec(kind="repair", rung=3, group_per_symbol=True), window_blocks=10)
    sizes, slices, repair = sch.repair(4)
    got = h.fetch((4 * 256, 0), (4 * 256 + 255, 1 << 62))
    assert [(o.group_id, o.object_id) for o in got] == [(4 * 256 + j, 0) for j in slices[2]]
    assert all(o.datagram for o in got)
    assert h.last_object(4 * 256 + slices[2][0]) == 0


def test_history_never_answers_with_status_objects():
    sch = schedule()
    sch.last_out = 3
    h = ScheduleHistory(sch, FetchSpec(kind="repair", rung=1, end_of_group=True), window_blocks=10)
    got = h.fetch((2, 0), (2, 1 << 62))
    assert got and all(o.status == 0 for o in got)


def test_whole_group_past_the_window_marker_fits_a_varint():
    """aiomoqt resolves a whole End Group to object 1 << 62; a marker must not carry that."""
    sch = schedule()
    sch.last_out = 4
    h = ScheduleHistory(sch, FetchSpec(kind="repair", rung=2), window_blocks=1)   # holds block 4
    got = h.fetch((1, 0), (2, 1 << 62))
    assert len(got) == 1 and got[0].end_of_range is not None
    sizes, slices, repair = sch.repair(2)
    assert (got[0].group_id, got[0].object_id) == (2, slices[1][-1])
    got[0]._serialize_d16()                                  # raises on a value >= 2**62
