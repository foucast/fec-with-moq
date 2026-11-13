"""BlockSchedule encoding: incremental ISA-L must equal a batch encode, and most of the
parity must already be built when the block's last source object is written."""
import pytest

from fecmoq.codec import Coder
from fecmoq.moq import BlockSchedule, rung_slices
from fecmoq.symbols import SPAN, layout_block, to_symbols

try:
    Coder(2, 1, "isal").encode([b"ab", b"cd"])
    HAVE_ISAL = True
except Exception:          # library not installed
    HAVE_ISAL = False


def _schedule(monkeypatch, coder, mode=None):
    monkeypatch.setenv("FECMOQ_CODER", coder)
    if mode:
        monkeypatch.setenv("FECMOQ_ENCODE", mode)
    else:
        monkeypatch.delenv("FECMOQ_ENCODE", raising=False)
    return BlockSchedule(seed=7, n_blocks=5, n_objects=12, gap_s=0.2)


@pytest.mark.skipif(not HAVE_ISAL, reason="ISA-L not installed")
@pytest.mark.parametrize("b", range(5))
def test_incremental_equals_batch(monkeypatch, b):
    sch = _schedule(monkeypatch, "isal")
    assert sch.encode_mode == "incremental"
    objs = sch.begin_block(b)
    lay = layout_block([len(o) for o in objs], SPAN)
    fed_before_last = None
    for i in range(len(objs)):
        if i == len(objs) - 1:
            fed_before_last = sch._inc[b]["fed"]
        sch.object_written(b, i)
    sch.finish_block(b)
    sizes, slices, rep = sch.repair(b)
    want = Coder(lay.k, slices[-1].stop, "isal").encode(to_symbols(lay, objs))
    assert rep == want
    # before the last object, every symbol that ends at or before its start was fed
    assert fed_before_last == lay.object_starts[-1] // lay.symbol_size
    assert fed_before_last > 0
    assert b in sch.encode_s and b in sch.encode_total_s


@pytest.mark.skipif(not HAVE_ISAL, reason="ISA-L not installed")
def test_symbol_feed_for_datagram_source(monkeypatch):
    sch = _schedule(monkeypatch, "isal")
    objs = sch.begin_block(3)
    lay = layout_block([len(o) for o in objs], SPAN)
    for j in range(lay.k):
        sch.symbol_written(3, j)
    sch.finish_block(3)
    slices = rung_slices(lay.k, sch.rung_fracs)
    assert sch.repair(3)[2] == Coder(lay.k, slices[-1].stop, "isal").encode(to_symbols(lay, objs))


@pytest.mark.skipif(not HAVE_ISAL, reason="ISA-L not installed")
def test_isal_batch_mode(monkeypatch):
    sch = _schedule(monkeypatch, "isal", "batch")
    assert sch.encode_mode == "batch"
    objs = sch.begin_block(0)
    for i in range(len(objs)):
        sch.object_written(0, i)        # no-ops in batch mode
    sch.finish_block(0)
    assert 0 not in sch.encode_s        # nothing encoded until a rung asks
    sch.repair(0)
    assert 0 in sch.encode_s


def test_zfec_stays_batch(monkeypatch):
    sch = _schedule(monkeypatch, "zfec")
    assert (sch.coder, sch.encode_mode) == ("zfec", "batch")
    with pytest.raises(ValueError):
        _schedule(monkeypatch, "zfec", "incremental")


@pytest.mark.parametrize("policy", ["sent", "queued"])
def test_release_after_source(monkeypatch, policy):
    import asyncio
    monkeypatch.setenv("FECMOQ_REPAIR_AFTER", policy)
    sch = _schedule(monkeypatch, "zfec")
    assert sch.repair_after == policy

    class _Out:                       # no lanes, nothing queued
        lanes = ()

    async def go():
        await sch.release_after_source(4, _Out())
        await asyncio.wait_for(sch.wait_source_out(4), 1.0)
    sch.has_source = True
    asyncio.run(go())
    assert sch.last_out == 4
    assert (4 in sch.sent_wait_s) == (policy == "sent")
    assert sch.sent_timeouts == 0


def test_repair_policy_rejects_unknown(monkeypatch):
    monkeypatch.setenv("FECMOQ_REPAIR_AFTER", "whenever")
    with pytest.raises(ValueError):
        _schedule(monkeypatch, "zfec")
