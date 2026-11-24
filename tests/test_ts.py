"""MPEG-TS split into objects/groups/FEC blocks, and reassembly (fecmoq.ts, fecmoq.ts_moq)."""
import asyncio
import os
import struct

import pytest

from fecmoq.ts import (OBJECT_BYTES, TS_PACKET, PsiTracker, TsBlocks, TsPacketizer,
                       TsReassembler, object_times, pcr_of, pid_of, psi_each_group_default,
                       random_access, split_ts)

VPID, APID, PMT = 0x100, 0x101, 0x1000


def _crc_stub(section: bytes) -> bytes:
    return section + b"\0\0\0\0"                     # the parser doesn't check the CRC


def _psi_packet(pid: int, section: bytes, cc: int) -> bytes:
    body = b"\x00" + section                          # pointer_field
    hdr = bytes([0x47, 0x40 | (pid >> 8), pid & 0xFF, 0x10 | (cc & 0xF)])
    return (hdr + body).ljust(TS_PACKET, b"\xff")


def pat(cc: int = 0) -> bytes:
    prog = struct.pack("!HH", 1, 0xE000 | PMT)
    n = 5 + len(prog) + 4
    sec = bytes([0x00, 0xB0 | (n >> 8), n & 0xFF]) + b"\x00\x01\xc1\x00\x00" + prog
    return _psi_packet(0, _crc_stub(sec), cc)


def pmt(cc: int = 0) -> bytes:
    es = bytes([0x1B, 0xE0 | (VPID >> 8), VPID & 0xFF, 0xF0, 0x00,
                0x0F, 0xE0 | (APID >> 8), APID & 0xFF, 0xF0, 0x00])
    body = b"\x00\x01\xc1\x00\x00" + bytes([0xE0 | (VPID >> 8), VPID & 0xFF, 0xF0, 0x00]) + es
    n = len(body) + 4
    sec = bytes([0x02, 0xB0 | (n >> 8), n & 0xFF]) + body
    return _psi_packet(PMT, _crc_stub(sec), cc)


def es_packet(pid: int, cc: int, *, rai=False, pcr=None, tag=0) -> bytes:
    af = b""
    if rai or pcr is not None:
        flags = (0x40 if rai else 0) | (0x10 if pcr is not None else 0)
        f = bytes([flags])
        if pcr is not None:
            base, ext = pcr // 300, pcr % 300
            f += struct.pack("!IH", base >> 1, ((base & 1) << 15) | 0x7E00 | ext)
        af = bytes([len(f)]) + f
    afc = 0x30 if af else 0x10
    hdr = bytes([0x47, pid >> 8, pid & 0xFF, afc | (cc & 0xF)])
    payload = struct.pack("!I", tag)
    return (hdr + af + payload).ljust(TS_PACKET, b"\xaa")


def null_packet() -> bytes:
    return bytes([0x47, 0x1F, 0xFF, 0x10]).ljust(TS_PACKET, b"\xff")


def synth_ts(groups=5, packets_per_group=50, pre=0, psi_mid=False, nulls_at=()):
    """PAT, PMT, then `groups` GOPs of video (RAI on the first packet of each) and audio,
    a PCR on video every 10 packets at 6 Mb/s; `pre` video packets with no RAI before the
    first GOP (a stream joined mid-GOP); `psi_mid`: the PAT and PMT again in the middle of
    every GOP, as a broadcast mux repeats them; `nulls_at`: a null packet after each of these
    packet indexes of every GOP (CBR stuffing)."""
    out = [pat(), pmt()]
    psi_cc = 1
    cc = 0
    tag = 0
    pcr_per_pkt = int(27e6 * 188 * 8 / 6e6)
    n = 0
    for i in range(pre):
        out.append(es_packet(VPID, cc, tag=tag)); cc += 1; tag += 1; n += 1
    for g in range(groups):
        for i in range(packets_per_group):
            pid = APID if i % 9 == 8 else VPID
            pcr = (n * pcr_per_pkt) if (pid == VPID and i % 10 == 0) else None
            out.append(es_packet(pid, cc, rai=(i == 0), pcr=pcr, tag=tag))
            cc += 1; tag += 1; n += 1
            if i in nulls_at:
                out.append(null_packet()); n += 1
            if psi_mid and i == packets_per_group // 2:
                out += [pat(psi_cc % 16), pmt(psi_cc % 16)]
                psi_cc += 1
    return b"".join(out)


def test_packet_fields():
    p = es_packet(VPID, 3, rai=True, pcr=123456789)
    assert pid_of(p) == VPID and random_access(p) and pcr_of(p) == 123456789
    q = es_packet(APID, 1)
    assert not random_access(q) and pcr_of(q) is None
    psi = PsiTracker()
    psi.feed(pat()); psi.feed(pmt())
    assert (psi.pmt_pid, psi.video_pid, psi.pcr_pid) == (PMT, VPID, VPID)


def test_split_groups_at_random_access_points():
    data = synth_ts(groups=4, packets_per_group=50)
    objs = split_ts(data)
    assert b"".join(o.data for o in objs) == data                # nothing lost or reordered
    assert all(len(o.data) % TS_PACKET == 0 and len(o.data) <= OBJECT_BYTES for o in objs)
    starts = [i for i, o in enumerate(objs) if o.group_start]
    assert starts[0] == 0 and len(starts) == 4                   # one group per GOP
    first = [objs[0].data[i:i + TS_PACKET] for i in range(0, len(objs[0].data), TS_PACKET)]
    assert pid_of(first[0]) == 0 and pid_of(first[1]) == PMT and random_access(first[2])
    for i in starts[1:]:
        first = objs[i].data[:TS_PACKET]
        assert pid_of(first) == VPID and random_access(first)    # the RAP packet leads the group
        assert len(objs[i - 1].data) <= OBJECT_BYTES             # cut short before it if needed
    # a full object is 7 packets everywhere except just before a group start or at the end
    for i, o in enumerate(objs[:-1]):
        if not objs[i + 1].group_start:
            assert len(o.data) == OBJECT_BYTES


def test_incremental_feed_matches_one_shot():
    data = synth_ts(groups=3, packets_per_group=40)
    p = TsPacketizer()
    objs = []
    for i in range(0, len(data), 1000):                          # odd chunking, mid-packet cuts
        objs += p.feed(data[i:i + 1000])
    objs += p.flush()
    assert [(o.data, o.group_start) for o in objs] == [(o.data, o.group_start) for o in split_ts(data)]


def test_drop_until_rap():
    data = synth_ts(groups=2, packets_per_group=30, pre=11)
    objs = split_ts(data, drop_until_rap=True)
    assert objs[0].group_start and random_access(objs[0].data[:TS_PACKET])
    assert sum(len(o.data) for o in objs) == len(data) - 13 * TS_PACKET   # PAT, PMT, 11 pre


def test_object_times_from_pcr():
    objs = split_ts(synth_ts(groups=3, packets_per_group=70))
    t = object_times(objs)
    assert t[0] == 0 and all(b >= a for a, b in zip(t, t[1:]))
    per_obj = 7 * 188 * 8 / 6e6
    assert abs(t[-1] / (len(objs) - 1) - per_obj) / per_obj < 0.2


def test_blocks_within_groups():
    objs = split_ts(synth_ts(groups=3, packets_per_group=100))
    tb = TsBlocks(4)
    ids = []
    for o in objs:
        b, oid, _closed = tb.add(o, 0.0)
        ids.append((tb.blocks[b].group, oid))
    tb.end()
    # object ids restart at 0 in each group and have no holes
    for g in {g for g, _ in ids}:
        os_ = [o for gg, o in ids if gg == g]
        assert os_ == list(range(len(os_)))
    for blk in tb.blocks:
        assert blk.closed and 1 <= len(blk.objects) <= 4
        assert blk.first_object == blk.q * 4
        if len(blk.objects) < 4:
            assert blk.ends_group
    assert all(blk.index == i for i, blk in enumerate(tb.blocks))


# ------------------------------------------------------------ reassembly

def _publish(data, n=4, rung=0.5, per_object=7):
    """(source objects [(g, o, payload)], repair [(g, obj, payload)], END_OF_GROUP marks)
    as the publisher would send them, with ISA-L/zfec repair at `rung` of each block."""
    from fecmoq.codec import Coder, default_backend
    from fecmoq.moq import OBJECT_STRIDE, rung_slices
    from fecmoq.symbols import SPAN, layout_block, to_symbols
    from fecmoq.wire import pack_repair
    tb = TsBlocks(n)
    src, rep = [], []
    for o in split_ts(data, per_object=per_object):
        b, oid, _ = tb.add(o, 0.0)
        src.append((tb.blocks[b].group, oid, o.data))
    tb.end()
    for blk in tb.blocks:
        sizes = [len(x) for x in blk.objects]
        lay = layout_block(sizes, SPAN)
        sl = rung_slices(lay.k, [rung])
        sym = Coder(lay.k, sl[-1].stop, default_backend()).encode(to_symbols(lay, blk.objects))
        for j in sl[0]:
            rep.append((blk.group, blk.q * OBJECT_STRIDE + j, pack_repair(sizes, sym[j])))
    eog = {}
    for g, o, _ in src:
        eog[g] = max(eog.get(g, 0), o + 1)
    return src, rep, eog


def _run(r, src, rep, eog, drop=(), order=None, t_step=0.001, end_t=10.0):
    out = []
    t = 0.0
    items = [("s", x) for x in src] + [("r", x) for x in rep]
    if order:
        items = order(items)
    for kind, (g, o, p) in items:
        t += t_step
        if kind == "s":
            if (g, o) in drop:
                continue
            r.add_source(g, o, p, t)
        else:
            r.add_repair(g, o, p, t)
        r.poll(t, out.append)
    for g, n in eog.items():
        r.end_of_group(g, n)
    r.poll(end_t, out.append, final=True)
    return b"".join(out)


def test_reassemble_no_loss_is_byte_exact():
    data = synth_ts(groups=4, packets_per_group=60)
    src, rep, eog = _publish(data)
    r = TsReassembler(4, window=0.5)
    assert _run(r, src, rep, eog) == data
    assert r.stats["lost"] == 0 and r.stats["fec"] == 0


def test_reassemble_fec_repairs_a_lost_object():
    data = synth_ts(groups=3, packets_per_group=60)
    src, rep, eog = _publish(data, rung=0.5)
    drop = {(1, 2), (2, 5)}
    r = TsReassembler(4, window=0.5)
    assert _run(r, src, rep, eog, drop=drop) == data
    assert r.stats["fec"] == 2 and r.stats["lost"] == 0


def test_reassemble_skips_unrecoverable_in_order_and_logs_it():
    data = synth_ts(groups=3, packets_per_group=60)
    src, _rep, eog = _publish(data)
    drop = {(1, 3)}
    r = TsReassembler(4, window=0.05)
    got = _run(r, src, [], eog, drop=drop, t_step=0.01)
    want = b"".join(p for g, o, p in src if (g, o) not in drop)
    assert got == want                                    # no filler, the rest in order
    assert r.lost == [(1, 3)] and r.stats["lost"] == 1


def test_reassemble_out_of_order_arrival():
    data = synth_ts(groups=3, packets_per_group=60)
    src, rep, eog = _publish(data)

    def shuffle(items):
        s = [x for x in items if x[0] == "s"]
        # swap neighbours within each group: arrival order differs from object order
        for i in range(0, len(s) - 1, 2):
            if s[i][1][0] == s[i + 1][1][0]:
                s[i], s[i + 1] = s[i + 1], s[i]
        return s + [x for x in items if x[0] == "r"]
    r = TsReassembler(4, window=0.5)
    assert _run(r, src, rep, eog, order=shuffle) == data


def test_reassemble_fetch_fills_what_fec_cannot():
    data = synth_ts(groups=2, packets_per_group=60)
    src, _rep, eog = _publish(data)
    by_id = {(g, o): p for g, o, p in src}
    drop = {(1, 1), (1, 2)}
    r = TsReassembler(4, window=0.02, fetch=True, fetch_wait=1.0)
    out = []
    t = 0.0
    for g, o, p in src:
        t += 0.01
        if (g, o) not in drop:
            r.add_source(g, o, p, t)
        r.poll(t, out.append)
        for fg, o0, o1 in r.want_fetch():
            for oo in range(o0, o1):
                r.add_source(fg, oo, by_id[(fg, oo)], t, how="fetched")
    for g, n in eog.items():
        r.end_of_group(g, n)
    r.poll(t + 5, out.append, final=True)
    assert b"".join(out) == data
    assert r.stats["fetched"] == 2 and r.stats["fetch_requests"] == 1


def test_reassemble_whole_block_lost_counts_its_objects():
    data = synth_ts(groups=2, packets_per_group=60)
    src, _rep, eog = _publish(data)
    drop = {(1, o) for o in range(4, 8)}               # block q=1 of group 1, every object
    r = TsReassembler(4, window=0.05)
    got = _run(r, src, [], eog, drop=drop, t_step=0.01)
    assert got == b"".join(p for g, o, p in src if (g, o) not in drop)
    assert sorted(r.lost) == sorted(drop)


# ------------------------------------------------------------ the publisher schedule

def test_ts_schedule_addresses_and_fetch_history():
    from fecmoq.moq import FetchSpec, OBJECT_STRIDE, ScheduleHistory, track_objects
    from fecmoq.ts_moq import TsSchedule
    from aiomoqt.types import ObjectStatus

    async def go():
        sch = TsSchedule(block_objects=4, rung_fracs=[0.25, 0.25], nominal_rate_bps=6e6, pace=True)
        sch.load_file_objects(split_ts(synth_ts(groups=3, packets_per_group=50)))
        return sch
    sch = asyncio.run(go())
    spec = FetchSpec(kind="source", source_mapping="group")
    seen = {}
    for b in range(len(sch.tb.blocks)):
        for g, o, sg, p, st in track_objects(sch, spec, b):
            if st is None:
                assert seen.setdefault(g, -1) == o - 1      # contiguous object ids per group
                seen[g] = o
            else:
                assert st == ObjectStatus.END_OF_GROUP and sch.block_ends_group(b)
        sizes, slices, _rep = sch.repair(b)
        g, o, first = sch.repair_address(b, 0, slices[0][0])
        blk = sch.tb.blocks[b]
        assert (g, o // OBJECT_STRIDE) == (blk.group, blk.q) and first == (blk.q == 0)
    sch.last_out = len(sch.tb.blocks) - 1
    h = ScheduleHistory(sch, spec, window_blocks=1000)
    objs = h.fetch((1, 2), (1, 5))
    assert [(x.group_id, x.object_id) for x in objs] == [(1, 2), (1, 3), (1, 4), (1, 5)]
    assert h.last_object(1) == seen[1]


def test_ts_schedule_refuses_blocks_whose_repair_overflows_a_datagram():
    from fecmoq.ts_moq import TsSchedule

    async def go(n):
        return TsSchedule(block_objects=n, rung_fracs=[0.15], nominal_rate_bps=6e6, pace=True)
    assert asyncio.run(go(24)).n_objects == 24          # 2 + 48 + 1100 = 1150 B
    with pytest.raises(ValueError, match="datagram ceiling"):
        asyncio.run(go(32))                             # 1166 B: the bug the first sweep hit


def test_window_shorter_than_a_block_does_not_cut_its_tail():
    """Objects of a block trickle in over longer than the window: nothing is lost or
    reordered, because a block's wait starts only when a later block starts arriving."""
    data = synth_ts(groups=2, packets_per_group=80)
    src, rep, eog = _publish(data)
    r = TsReassembler(4, window=0.005)
    assert _run(r, src, rep, eog, t_step=0.004) == data      # 16 ms per block > 5 ms window
    assert r.stats["lost"] == 0 and r.stats["late"] == 0


def test_lost_objects_are_counted_when_the_window_expires():
    data = synth_ts(groups=2, packets_per_group=80)
    src, _rep, eog = _publish(data)
    drop = {(1, 5), (1, 9)}
    r = TsReassembler(4, window=0.005)
    got = _run(r, src, [], eog, drop=drop, t_step=0.004)
    assert got == b"".join(p for g, o, p in src if (g, o) not in drop)
    assert sorted(r.lost) == sorted(drop) and r.stats["objects_out"] + r.stats["lost"] == len(src)


def test_fetch_waits_overlap_across_lost_blocks():
    """A run of blocks whose source never came (repair only) is fetched at once, so the output
    waits one fetch_wait for the whole run, not one per block."""
    data = synth_ts(groups=3, packets_per_group=200)
    src, rep, eog = _publish(data)
    drop = {(g, o) for g, o, _ in src if g == 1}            # all of group 1's source
    r = TsReassembler(4, window=0.01, fetch=True, fetch_wait=0.5)
    out, t = [], 0.0
    for kind, (g, o, p) in [("r", x) for x in rep] + [("s", x) for x in src]:
        t += 0.001
        if kind == "s" and (g, o) not in drop:
            r.add_source(g, o, p, t)
        elif kind == "r":
            r.add_repair(g, o, p, t)
    for _ in range(100):                                    # 1 s of polling after it all came
        t += 0.01
        r.poll(t, out.append)
        r.want_fetch()
    assert r.cursor[0] >= 2                                 # group 1 done within ~0.5 s, not 0.5 s x blocks
    assert r.stats["lost"] + r.stats["fec"] == len(drop)  # a one-symbol block decodes from repair


def test_psi_each_group_leads_later_groups_with_pat_and_pmt():
    data = synth_ts(groups=4, packets_per_group=30)
    plain = split_ts(data)
    objs = split_ts(data, psi_each_group=True)
    starts = [o for o in objs if o.group_start]
    assert len(starts) == 4
    # group 0 already starts with the stream's own PAT/PMT; group 1 gains copies of them. The
    # stream sends no PAT/PMT after that, so groups 2 and 3 get none: a packet may be
    # duplicated once only (ISO/IEC 13818-1 2.4.3.3)
    assert starts[0].data == [o for o in plain if o.group_start][0].data
    assert starts[1].data[:TS_PACKET] == pat() and starts[1].data[TS_PACKET:2 * TS_PACKET] == pmt()
    assert random_access(starts[1].data[2 * TS_PACKET:3 * TS_PACKET])
    for o in starts[2:]:
        assert random_access(o.data[:TS_PACKET])
    out = b"".join(o.data for o in objs)
    assert len(out) == len(data) + 2 * TS_PACKET
    # removing the copies gives back the input, byte for byte
    pkts = [out[i:i + TS_PACKET] for i in range(0, len(out), TS_PACKET)]
    kept, prev = [], {}
    for p in pkts:
        if prev.get(pid_of(p)) == p:
            continue                                  # a duplicate of the PID's last packet
        prev[pid_of(p)] = p
        kept.append(p)
    assert b"".join(kept) == data


def test_psi_each_group_incremental_matches_one_shot():
    data = synth_ts(groups=5, packets_per_group=40, psi_mid=True)
    p = TsPacketizer(psi_each_group=True)
    objs = []
    for i in range(0, len(data), 1000):
        objs += p.feed(data[i:i + 1000])
    objs += p.flush()
    assert [o.data for o in objs] == [o.data for o in split_ts(data, psi_each_group=True)]
    assert p.psi_inserted == 8


def test_catalog_has_no_mux_rate_in_unmodified_mode():
    import json
    from fecmoq.ts import catalog_json
    cat = json.loads(catalog_json(source="src", rungs=[0.15], block_objects=24, symbol_size=1100,
                                  mux_rate=6_000_000, pcr_pid=0x100, object_stride=256))
    src = cat["tracks"][0]
    assert src["mpeg2tsMode"] == "unmodified-program" and "mpeg2tsMuxRate" not in src
    assert src["bitrate"] == 6_000_000


def test_group_tail_that_comes_after_the_group_end_grace_counts_as_lost():
    # a group's last block delayed past the group-end grace: the cursor has moved on to the
    # next group, so the tail is dropped; it must be counted as lost, not vanish
    data = synth_ts(groups=3, packets_per_group=60)
    src, _rep, eog = _publish(data)
    last0 = max(o for g, o, _ in src if g == 0)
    tail = [(g, o, p) for g, o, p in src if g == 0 and o // 4 == last0 // 4]
    rest = [x for x in src if x not in tail]
    k = next(i for i, (g, o, p) in enumerate(rest) if g == 2)
    late_order = rest[:k] + tail + rest[k:]          # the tail turns up only once group 2 does
    r = TsReassembler(4, window=0.01, group_end_grace=0.01)
    out, t = [], 0.0
    for g, o, p in late_order:
        t += 0.01
        r.add_source(g, o, p, t)
        r.poll(t, out.append)
    for g, n in eog.items():
        r.end_of_group(g, n)
    r.poll(t + 1, out.append, final=True)
    assert b"".join(out) == b"".join(p for g, o, p in src if (g, o, p) not in tail)
    assert sorted(r.lost) == sorted((g, o) for g, o, _ in tail)
    assert r.stats["lost"] == len(tail) and r.stats["lost_after_group_end"] == len(tail)


def test_late_joiner_does_not_count_the_group_before_its_first():
    data = synth_ts(groups=3, packets_per_group=60)
    src, _rep, eog = _publish(data)
    joined = [x for x in src if not (x[0] == 0 and x[1] < 6)]   # joins mid group 0
    r = TsReassembler(4, window=0.05)
    got = _run(r, joined, [], eog, t_step=0.01)
    assert got == b"".join(p for g, o, p in src if g >= 1)
    assert r.lost == [] and r.stats["lost"] == 0


def test_datagram_source_objects_fit_and_groups_close_with_end_of_group():
    # --source-mapping datagram: objects of 6 packets (1,128 B, under the 1,152 B ceiling),
    # one per datagram; each group's END_OF_GROUP status follows its last object, in the
    # live track and in what FETCH serves
    from aiomoqt.delivery import StreamMapping
    from aiomoqt.types import ObjectStatus
    from fecmoq.moq import EOG_ONLY, FetchSpec, track_objects
    from fecmoq.ts_moq import TsSchedule, TsSourceTrack
    from fecmoq.wire import DATAGRAM_MAX_PAYLOAD

    class Out:
        def __init__(self):
            self.w = []

        async def write(self, g, o, data, group_start=False, extensions=None):
            self.w.append((g, o, len(data), extensions is EOG_ONLY))

    class Track:
        mapping = StreamMapping.DATAGRAM
        EOG_COPIES = TsSourceTrack.EOG_COPIES

    async def go():
        sch = TsSchedule(block_objects=4, rung_fracs=[0.25], nominal_rate_bps=6e6, pace=False,
                         packets_per_object=6)
        sch.repair_after = "queued"
        sch.load_file_objects(split_ts(synth_ts(groups=3, packets_per_group=50), per_object=6))
        t = Track()
        t.schedule = sch
        out = Out()
        await TsSourceTrack.produce(t, out)
        return sch, out.w
    sch, w = asyncio.run(go())
    assert max(n for _g, _o, n, eog in w if not eog) == 6 * TS_PACKET <= DATAGRAM_MAX_PAYLOAD
    for g in range(3):
        objs = [o for gg, o, _n, eog in w if gg == g and not eog]
        assert objs == list(range(len(objs)))
        # END_OF_GROUP three times (TsSourceTrack.EOG_COPIES), one block apart
        assert [(o, n) for gg, o, n, eog in w if gg == g and eog] == [(len(objs), 0)] * 3
    served = [x for b in range(len(sch.tb.blocks))
              for x in track_objects(sch, FetchSpec(kind="source", source_mapping="object-datagram"), b)]
    assert all(sg is None for _g, _o, sg, _p, _st in served)
    assert [(g, o) for g, o, _sg, _p, st in served if st == ObjectStatus.END_OF_GROUP] == \
           sorted({(g, o) for g, o, _n, eog in w if eog})


def test_datagram_source_reassembles_out_of_order_with_loss():
    # datagrams arrive in any order within a block and some never; FEC fills the holes
    data = synth_ts(groups=3, packets_per_group=60)
    tb = TsBlocks(4)
    for o in split_ts(data, per_object=6):
        tb.add(o, 0.0)
    tb.end()
    src, rep, eog = _publish(data, rung=0.5, per_object=6)

    def shuffle_blocks(items):
        import random
        rnd = random.Random(3)
        out = []
        for i in range(0, len(items), 5):
            chunk = items[i:i + 5]
            rnd.shuffle(chunk)
            out += chunk
        return out
    r = TsReassembler(4, window=0.5)
    assert _run(r, src, rep, eog, drop={(0, 1), (1, 6), (2, 3)}, order=shuffle_blocks) == data
    assert r.stats["fec"] == 3 and r.stats["lost"] == 0


def test_output_that_stops_counts_the_rest_of_the_track_as_lost():
    # the relay stops sending to this subscriber after group 1: TRACK_STATUS says the track
    # reached group 3, so group 2's objects (length known from its END_OF_GROUP? no: never
    # seen) are estimated and group 3's through the Largest Location are counted
    data = synth_ts(groups=4, packets_per_group=60)
    src, _rep, eog = _publish(data)
    seen = [x for x in src if x[0] <= 1]
    r = TsReassembler(4, window=0.05)
    got = _run(r, seen, [], {g: n for g, n in eog.items() if g <= 1}, t_step=0.01)
    assert got == b"".join(p for g, o, p in seen) and r.stats["lost"] == 0
    r.track_end(3, eog[3] - 1)
    assert r.stats["lost"] == eog[3] and r.stats["lost_after_output_end"] == eog[3]
    assert r.stats["lost_unseen_est"] == eog[2] and r.stats["truncated_at"] == [1, eog[1]]
    # nothing more once the output reached the track's end (the Largest Location can be the
    # END_OF_GROUP status, one past the last object)
    r2 = TsReassembler(4, window=0.05)
    _run(r2, src, [], eog, t_step=0.01)
    r2.track_end(3, eog[3])
    assert r2.stats["lost"] == 0 and "truncated_at" not in r2.stats
    r2.end_of_group(4, 0)                            # END_OF_TRACK: an empty group 4
    r2.track_end(4, 0)
    assert r2.stats["lost"] == 0 and "truncated_at" not in r2.stats
    # its END_OF_TRACK datagram lost, but the publisher said the track ended
    r3 = TsReassembler(4, window=0.05)
    _run(r3, src, [], eog, t_step=0.01)
    r3.track_end(4, 0, track_ended=True)
    assert r3.stats["lost"] == 0 and "truncated_at" not in r3.stats


def test_last_block_of_the_track_with_a_hole_times_out_from_its_end_of_group():
    # no later block will ever come: the window starts at the group's END_OF_GROUP, so a hole
    # in the track's last block is FETCHed and released, not held until the subscriber exits
    data = synth_ts(groups=2, packets_per_group=60)
    src, _rep, eog = _publish(data)
    last = max(src, key=lambda x: (x[0], x[1]))
    r = TsReassembler(4, window=0.1, fetch=True, fetch_wait=0.2)
    out, t = [], 0.0
    for g, o, p in src:
        t += 0.001
        if (g, o) != (last[0], last[1] - 1):
            r.add_source(g, o, p, t)
        r.poll(t, out.append)
    for g, n in eog.items():
        r.end_of_group(g, n, t)
    r.poll(t + 0.15, out.append)
    assert r.want_fetch() == [(last[0], last[1] - 1, last[1])]
    r.add_source(last[0], last[1] - 1, dict(((g, o), p) for g, o, p in src)[(last[0], last[1] - 1)],
                 t + 0.2, how="fetched")
    r.poll(t + 0.2, out.append)
    assert b"".join(out) == data and r.stats["fetched"] == 1


def test_psi_each_group_with_datagram_sized_objects():
    # --ts-psi-each-group with datagram source (6 packets per object): the PAT/PMT copies count
    # against the object's packets, so no object outgrows a datagram, and each later group
    # still opens PAT, PMT, random access point
    from fecmoq.wire import DATAGRAM_MAX_PAYLOAD
    data = synth_ts(groups=4, packets_per_group=30, psi_mid=True)
    objs = split_ts(data, per_object=6, psi_each_group=True)
    assert max(len(o.data) for o in objs) <= 6 * TS_PACKET <= DATAGRAM_MAX_PAYLOAD
    starts = [o for o in objs if o.group_start]
    for g, o in enumerate(starts[1:]):
        assert o.data[:TS_PACKET] == pat(g + 1) and o.data[TS_PACKET:2 * TS_PACKET] == pmt(g + 1)
        assert random_access(o.data[2 * TS_PACKET:3 * TS_PACKET])
    assert len(b"".join(o.data for o in objs)) == len(data) + 3 * 2 * TS_PACKET


# ------------------------------------------------------------ long files, read a piece at a time

def test_pcr_clock_matches_object_times():
    from fecmoq.ts import PcrClock
    objs = split_ts(synth_ts(groups=4, packets_per_group=90, pre=13))
    clock = PcrClock()
    got = [x for o in objs for x in clock.push(o)] + clock.flush()
    assert [o for o, _ in got] == objs                      # every object, in order, once
    want = object_times(objs)
    # between the first and the last PCR, the same interpolation; outside it, object_times
    # extrapolates at the whole file's mean rate, PcrClock at the first interval's (the lead)
    # or the mean so far (the tail), so only close
    pc = [i for i, o in enumerate(objs) if o.pcr_first is not None]
    t0 = got[pc[0]][1] - want[pc[0]]
    for i, ((_o, t), w) in enumerate(zip(got, want)):
        assert abs(t - t0 - w) < (1e-9 if pc[0] <= i <= pc[-1] else 5e-3)


def test_pcr_clock_vbr_and_wrap():
    from fecmoq.ts import PCR_HZ, PCR_WRAP, PcrClock, TsObject
    # PCRs 1 s apart over 2, 5, 3 objects (a VBR stream), the 33-bit base wrapping in between
    p0 = PCR_WRAP - PCR_HZ // 2
    pcrs = {0: p0, 2: (p0 + PCR_HZ) % PCR_WRAP, 7: (p0 + 2 * PCR_HZ) % PCR_WRAP,
            10: (p0 + 3 * PCR_HZ) % PCR_WRAP}
    objs = [TsObject(bytes([i]), i == 0, pcrs.get(i)) for i in range(12)]
    clock = PcrClock()
    got = [x for o in objs for x in clock.push(o)] + clock.flush()
    t = [round(x, 6) for _o, x in got]
    assert t[:11] == [0, 0.5, 1, 1.2, 1.4, 1.6, 1.8, 2, 2 + 1 / 3, 2 + 2 / 3, 3][:11] or \
        t[:11] == [round(x, 6) for x in (0, 0.5, 1, 1.2, 1.4, 1.6, 1.8, 2, 2 + 1 / 3, 2 + 2 / 3, 3)]
    assert t[11] == round(3 + 0.3, 6)                       # the tail at the mean rate (3 s / 10)


def test_feed_file_streams_like_load(tmp_path):
    """feed_file() (a piece at a time, paced) gives the blocks load_file_objects() gives,
    hashes the file, and drops the objects of blocks far enough behind."""
    import hashlib
    from fecmoq.ts_moq import TsSchedule
    data = synth_ts(groups=6, packets_per_group=80, pre=5)
    path = tmp_path / "v.ts"
    path.write_bytes(data)

    async def load():
        s = TsSchedule(block_objects=4, rung_fracs=[0.25], nominal_rate_bps=6e6, pace=True)
        s.load_file_objects(split_ts(data))
        return s

    async def feed():
        s = TsSchedule(block_objects=4, rung_fracs=[0.25], nominal_rate_bps=6e6, pace=True)
        s.t_start = __import__("time").monotonic() - 100      # far behind: no waiting
        await s.feed_file(str(path), chunk=188 * 7, keep_s=1e9)
        return s
    a, b = asyncio.run(load()), asyncio.run(feed())
    assert [(x.group, x.first_object, x.objects, x.ends_group) for x in a.tb.blocks] == \
        [(x.group, x.first_object, x.objects, x.ends_group) for x in b.tb.blocks]
    # times: the same up to the ends' extrapolation (test_pcr_clock_matches_object_times)
    assert max(abs(p - q) for x, y in zip(a.tb.blocks, b.tb.blocks) for p, q in zip(x.times, y.times)) < 5e-3
    assert b.sha256 == hashlib.sha256(data).hexdigest() and b.bytes_in == len(data)
    b.evict_before(b.tb.blocks[3].times[-1] + 1e-9)
    assert b.evicted == 4 and b.block(0) == [] and b.evicted_reads == 1 and b.block(4)


def test_ts_file_info(tmp_path):
    from fecmoq.ts import ts_file_info
    data = synth_ts(groups=5, packets_per_group=200)
    path = tmp_path / "v.ts"
    path.write_bytes(data)
    i = ts_file_info(str(path), span=188 * 300)
    assert (i["size"], i["video_pid"], i["pcr_pid"]) == (len(data), VPID, VPID)
    assert abs(i["rate_bps"] - 6e6) / 6e6 < 0.01


def test_feed_file_byte_range(tmp_path):
    import hashlib
    from fecmoq.ts_moq import TsSchedule
    data = synth_ts(groups=6, packets_per_group=80)
    path = tmp_path / "v.ts"
    path.write_bytes(data)
    start, length = 188 * 37, 188 * 200

    async def feed():
        s = TsSchedule(block_objects=4, rung_fracs=[0.25], nominal_rate_bps=6e6, pace=True)
        s.t_start = __import__("time").monotonic() - 100
        await s.feed_file(str(path), chunk=188 * 7, start=start, length=length)
        return s
    s = asyncio.run(feed())
    sent = b"".join(o for blk in s.tb.blocks for o in blk.objects)
    assert sent == data[start:start + length] and s.sha256 == hashlib.sha256(sent).hexdigest()


def test_scte35_pid_in_psi_and_catalog():
    import json
    from fecmoq.ts import catalog_json
    es = bytes([0x1B, 0xE1, 0x00, 0xF0, 0x00, 0x86, 0xE1, 0x02, 0xF0, 0x00, 0x86, 0xE1, 0x03, 0xF0, 0x00])
    body = b"\x00\x01\xc1\x00\x00" + bytes([0xE1, 0x00, 0xF0, 0x00]) + es
    n = len(body) + 4
    psi = PsiTracker()
    psi.feed(pat())
    psi.feed(_psi_packet(PMT, _crc_stub(bytes([0x02, 0xB0 | (n >> 8), n & 0xFF]) + body), 0))
    assert (psi.video_pid, psi.scte35_pids) == (0x100, [0x102, 0x103])
    cat = json.loads(catalog_json(source="src", rungs=[0.15], block_objects=24, symbol_size=1100,
                                  mux_rate=None, pcr_pid=0x100, object_stride=256,
                                  scte35_pids=psi.scte35_pids))
    t = cat["tracks"][0]
    assert t["mpeg2tsScte35Pid"] == 0x102 and t["mpeg2tsMode"] == "unmodified-program"
    assert "mpeg2tsMuxRate" not in t


def test_psi_each_group_default_off_where_the_ts_is_carried_unmodified():
    # a file is read as is and a URL source passed through: no PAT/PMT copies unless asked,
    # so the output stays byte-identical to the input
    assert psi_each_group_default("/data/feed.ts") == "off"
    assert psi_each_group_default("udp://239.1.1.1:5000") == "off"
    assert psi_each_group_default("srt://host:9000") == "off"
    # avgen: the publisher writes the mux itself, so the copies cost no byte identity
    assert psi_each_group_default("avgen") == "insert"


def test_relay_fec_pub_has_a_flag_for_each_direction():
    import subprocess
    import sys
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = dict(os.environ, PYTHONPATH=os.pathsep.join(
        [os.path.join(root, "src"), os.path.join(root, "vendor", "aiomoqt"),
         os.environ.get("PYTHONPATH", "")]))
    out = subprocess.run([sys.executable, os.path.join(root, "scripts", "relay_fec.py"), "pub", "--help"],
                         capture_output=True, text=True, env=env, timeout=60)
    assert out.returncode == 0, out.stderr
    assert "--ts-psi-each-group" in out.stdout and "--no-ts-psi-each-group" in out.stdout
    assert "insert|replace" in out.stdout and "--ts-psi-null-window" in out.stdout
    assert "--send-times" in out.stdout and "--no-send-times" in out.stdout


def test_psi_each_group_copies_each_original_once():
    # PAT/PMT repeated mid-GOP, as broadcast muxes do: every later group leads with copies of
    # the latest ones, and no packet appears more than twice in a row on its PID (the original
    # and one legal duplicate)
    data = synth_ts(groups=5, packets_per_group=40, psi_mid=True)
    objs = split_ts(data, psi_each_group=True)
    starts = [o for o in objs if o.group_start]
    assert len(starts) == 5
    for g, o in enumerate(starts[1:]):
        assert o.data[:TS_PACKET] == pat(g + 1) and o.data[TS_PACKET:2 * TS_PACKET] == pmt(g + 1)
    out = b"".join(o.data for o in objs)
    assert len(out) == len(data) + 4 * 2 * TS_PACKET
    run, prev = {}, {}
    for i in range(0, len(out), TS_PACKET):
        p = out[i:i + TS_PACKET]
        run[pid_of(p)] = run.get(pid_of(p), 0) + 1 if prev.get(pid_of(p)) == p else 1
        prev[pid_of(p)] = p
        assert run[pid_of(p)] <= 2


def test_psi_each_group_no_copy_twice_when_groups_come_faster_than_psi():
    # two random access points with no PAT/PMT between them (a short GOP at a scene cut): the
    # second group gets no copies rather than a second duplicate
    data = synth_ts(groups=3, packets_per_group=12, psi_mid=False)
    p = TsPacketizer(psi_each_group=True)
    objs = p.feed(data) + p.flush()
    assert p.psi_inserted == 2
    starts = [o for o in objs if o.group_start]
    assert starts[1].data[:TS_PACKET] == pat() and random_access(starts[2].data[:TS_PACKET])


def _packets(b: bytes) -> list[bytes]:
    return [b[i:i + TS_PACKET] for i in range(0, len(b), TS_PACKET)]


def test_psi_replace_keeps_every_packet_position():
    # CBR stuffing: nulls after packets 3, 7 and 20 of every GOP. Each later group's PAT and PMT
    # copies take the first two nulls after its start; nothing else moves
    data = synth_ts(groups=4, packets_per_group=30, psi_mid=True, nulls_at=(3, 7, 20))
    p = TsPacketizer(psi_each_group="replace")
    objs = p.feed(data) + p.flush()
    out = b"".join(o.data for o in objs)
    assert len(out) == len(data)
    src, dst = _packets(data), _packets(out)
    changed = [i for i in range(len(src)) if src[i] != dst[i]]
    assert len(changed) == 6 and p.psi_inserted == 6 and p.psi_skipped == 0
    assert all(pid_of(src[i]) == 0x1FFF for i in changed)
    assert [pid_of(dst[i]) for i in changed] == [0, PMT] * 3
    for i in changed:                      # each copy is its PID's previous packet, repeated
        prev = [q for q in dst[:i] if pid_of(q) == pid_of(dst[i])][-1]
        assert prev == dst[i]
    assert p.psi_null_offsets == [4, 9] * 3       # packets from the group start to each null


def test_psi_replace_with_no_null_in_reach_skips_and_logs():
    # VBR, no stuffing: nothing replaced, the output is the input, every skip logged
    data = synth_ts(groups=3, packets_per_group=30, psi_mid=True)
    logged = []
    p = TsPacketizer(psi_each_group="replace", null_window=10, log=logged.append)
    objs = p.feed(data) + p.flush()
    assert b"".join(o.data for o in objs) == data
    assert p.psi_inserted == 0 and p.psi_skipped == 4 and len(logged) == 2
    assert "no null packet in the 10 packets" in logged[0]


def test_psi_replace_drops_a_copy_whose_pid_sends_a_fresh_original_first():
    # the mid-GOP PAT/PMT come before the GOP's only null: their copies would no longer follow
    # their originals, so both are dropped and the null stays a null
    data = synth_ts(groups=3, packets_per_group=30, psi_mid=True, nulls_at=(20,))
    p = TsPacketizer(psi_each_group="replace")
    objs = p.feed(data) + p.flush()
    assert b"".join(o.data for o in objs) == data
    assert p.psi_superseded == 4 and p.psi_inserted == 0


def test_psi_replace_incremental_matches_one_shot():
    data = synth_ts(groups=5, packets_per_group=40, psi_mid=True, nulls_at=(2, 5, 30))
    p = TsPacketizer(psi_each_group="replace")
    objs = []
    for i in range(0, len(data), 1000):
        objs += p.feed(data[i:i + 1000])
    objs += p.flush()
    assert [o.data for o in objs] == [o.data for o in split_ts(data, psi_each_group="replace")]
    assert p.psi_inserted == 8


def test_psi_mode_must_be_known():
    with pytest.raises(ValueError):
        TsPacketizer(psi_each_group="sometimes")


def test_publisher_session_watchdog():
    # SESSION-DEAD: a recorded close, or a datagram limit fallen to 0 after the handshake gave one
    import importlib.util
    import sys
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path[:0] = [os.path.join(root, "src"), os.path.join(root, "vendor", "aiomoqt")]
    spec = importlib.util.spec_from_file_location("relay_fec", os.path.join(root, "scripts", "relay_fec.py"))
    rf = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(rf)

    class S:
        _close_err = None
        limit = 1152

        def datagram_max_payload(self):
            return self.limit
    s = S()
    assert rf.session_dead(s, 1152) is None
    s.limit = 0
    assert "datagram limit fell to 0" in rf.session_dead(s, 1152)
    assert rf.session_dead(s, 0) is None         # never had datagrams: not a sign of death
    s._close_err = (0x2, "idle timeout")
    assert "idle timeout" in rf.session_dead(s, 0)

    async def go():
        s2 = S()
        loop = asyncio.get_running_loop()
        loop.call_later(0.05, setattr, s2, "_close_err", (0, "gone"))
        return await asyncio.wait_for(rf.watch_session(s2, 1152, poll=0.01), 2)
    assert "gone" in asyncio.run(go())
