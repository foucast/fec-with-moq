"""aiomoqt tracks: a source track on subgroup streams, a repair track on datagrams.

An origin's tracks share one BlockSchedule. Live, repair cannot exist before the
whole source block does, so the schedule enforces that order: the source track
emits block b at its slot on an absolute timeline (start + b * gap), and the
repair tracks send block b only after b's source is out. The block is encoded
once and every rung sends its own slice of that one encode, so encoding never
delays the source.

The track layout this implements is described in DESIGN.md at the top of the repo.
"""
from __future__ import annotations

import asyncio
import bisect
import os
import random
import time

from aiomoqt.delivery import StreamMapping
from aiomoqt.track import PublishedTrack

from .codec import Coder, IsalIncrementalEncoder, default_backend
from .symbols import SPAN, erased_symbols, layout_block, to_symbols
from .synth import P_HI, P_LO, object_sizes, objects
from .wire import pack_repair

NAMESPACE = "fecmoq"
SOURCE_TRACK = "src"


def repair_track_name(rung: int) -> str:
    return f"rep{rung}"


def rung_slices(k: int, rung_fracs) -> list[range]:
    """Global repair-symbol index ranges for each rung (1-based rung = list position + 1).

    Rungs are ADDITIVE: rung r carries its own disjoint slice, and a subscriber on
    rungs 1..r holds the union. Symbols are interchangeable (any k of k+m decode),
    so which rungs contributed does not matter -- only how many symbols arrived.
    """
    out, start = [], 0
    for f in rung_fracs:
        n = max(1, round(k * f))
        out.append(range(start, start + n))
        start += n
    return out


def make_block(seed: int, block: int, n_objects: int, gop: int = 0) -> list[bytes]:
    """Block `block`'s source objects, deterministic from the seed.

    gop = 0 (default, the behaviour before 2026-10-06): an I-frame-sized object leads every
    block, so the block IS the GOP. gop > 0: one media timeline independent of the block --
    object j of the stream (j = block * n_objects + i) is I-frame-sized when j % gop == 0 and
    P-frame-sized otherwise, each object seeded by its own index. Blocks then cut that timeline
    every n_objects objects with no knowledge of frame types (short FEC blocks)."""
    if not gop:
        rng = random.Random(seed * 1_000_003 + block)
        return objects(object_sizes(n_objects, rng), rng)
    out = []
    for i in range(n_objects):
        j = block * n_objects + i
        rng = random.Random(seed * 1_000_003 + 7_919 * j + 1)
        size = object_sizes(1, rng)[0] if j % gop == 0 else rng.randint(P_LO, P_HI)
        out.append(rng.randbytes(size))
    return out


async def _drained(out):
    """Until every subscriber lane of `out` has taken all queued objects. `out` is
    aiomoqt's FanoutDelivery (or a wrapper exposing .lanes); without lanes there is
    nothing queued here."""
    while any(lane.queue.qsize() for lane in getattr(out, "lanes", ())):
        await asyncio.sleep(0.0005)


def _stream_bytes_unsent() -> int:
    """Stream bytes this process has handed to aiopquic and picoquic has not yet put
    into a packet: pushed - pulled - discarded over every per-stream TX ring. picoquic
    pulls a stream's bytes in its prepare_to_send callback, straight into the frame of
    a packet it is building, so 0 means all stream data has left the send buffer
    (sent -- not acknowledged; retransmission copies live inside picoquic)."""
    from aiopquic._binding._transport import tx_data_bytes_queued
    return tx_data_bytes_queued()


async def _source_sent(out, timeout_s: float) -> tuple[float, bool]:
    """Until the block's source has left the publisher's QUIC send buffer: every lane
    has taken its objects and no stream bytes are left unsent, on two checks 0.5 ms
    apart (the second catches a lane that has taken its last object but not pushed it
    yet). Process-wide is enough: the publisher has one connection and the block's
    source is the newest stream data. Capped at timeout_s so repair cannot starve.
    Returns (seconds waited, timed out)."""
    t0 = time.monotonic()
    quiet = 0
    while quiet < 2:
        if time.monotonic() - t0 > timeout_s:
            return time.monotonic() - t0, True
        empty = not any(lane.queue.qsize() for lane in getattr(out, "lanes", ()))
        quiet = quiet + 1 if empty and _stream_bytes_unsent() == 0 else 0
        await asyncio.sleep(0.0005)
    return time.monotonic() - t0, False


class BlockSchedule:
    """One origin's block timeline, shared by its source and repair tracks.

    - Block b is due at start + b * gap_s, start being when production begins, so
      per-block work does not stretch the period.
    - Repair for block b waits until b's source is out (written and taken by every
      subscriber lane). With no source track attached, repair goes at the block's
      slot -- nothing to wait for.
    - When repair may leave (FECMOQ_REPAIR_AFTER): "sent" (default) -- after the
      block's source has left this publisher's QUIC send buffer (_source_sent); MoQT
      priority does not cover datagrams, so the publisher enforces that repair ranks
      below its source. "queued" -- as soon as every subscriber lane has taken the
      source objects (the behaviour before 2026-10-02); with no encode delay, repair
      datagrams then overtake the block's stream data on the wire and delay it.
    - The block is encoded once, for every rung.
      batch (zfec, or ISA-L with FECMOQ_ENCODE=batch): encoded the first time any
        rung asks, after the source is out; synchronous, so it blocks the event loop
        for its duration.
      incremental (ISA-L, the default when FECMOQ_CODER=isal): parity is updated as
        the source track writes each object (begin_block / object_written /
        finish_block), so repair is ready ~immediately after the block's last source
        object and the encode is off the critical path.
      encode_s[b]: critical-path time -- the whole encode (batch), or the work left
      after the last source object (incremental). encode_total_s[b]: all encode work
      for the block.
    """

    KEEP = 8                                  # encoded blocks kept for slower rungs
    # Blocks per MoQ group. 1 (all but the GOP media schedule): group = block, object = symbol
    # index. Otherwise group = b // blocks_per_group, and symbol j of the group's q-th block is
    # Object q * OBJECT_STRIDE + j (address()).
    blocks_per_group = 1

    def address(self, b: int, j: int) -> tuple[int, int, bool]:
        """(group, object, starts a group) of symbol j of block b, source or repair."""
        if self.blocks_per_group == 1:
            return b, j, j == 0
        q = b % self.blocks_per_group
        return b // self.blocks_per_group, q * OBJECT_STRIDE + j, q == 0 and j == 0

    def repair_address(self, b: int, n: int, j: int) -> tuple[int, int, bool]:
        """(group, object, starts a group) of a rung's n-th repair symbol of block b, global
        repair index j. A group starts at each rung's FIRST symbol, not at j == 0: j is global,
        so only rung 1's symbols include j == 0, and a datagram lane that never sees a group
        start never opens (aiomoqt starts a new subscriber at the next group)."""
        g, o, _ = self.address(b, j)
        return g, o, n == 0 and b % self.blocks_per_group == 0

    def __init__(self, *, seed, n_blocks, n_objects, gap_s, rung_fracs=(0.15, 0.15, 0.15), gop=0):
        self.seed, self.n_blocks, self.n_objects, self.gap_s = seed, n_blocks, n_objects, gap_s
        self.gop = gop
        self.rung_fracs = list(rung_fracs)
        self.has_source = False
        self.t_start = None
        self.last_out = -1
        self._src_out: dict[int, asyncio.Event] = {}
        self._encoded: dict[int, tuple] = {}
        self.encode_s: dict[int, float] = {}
        self.encode_total_s: dict[int, float] = {}
        self.coder = default_backend()
        mode = os.environ.get("FECMOQ_ENCODE", "").strip().lower()
        self.encode_mode = mode or ("incremental" if self.coder == "isal" else "batch")
        if self.encode_mode not in ("batch", "incremental"):
            raise ValueError(f"FECMOQ_ENCODE={mode!r}; expected batch or incremental")
        if self.encode_mode == "incremental" and self.coder != "isal":
            raise ValueError("incremental encoding needs FECMOQ_CODER=isal")
        self._inc: dict[int, dict] = {}
        self.repair_after = os.environ.get("FECMOQ_REPAIR_AFTER", "sent").strip().lower()
        if self.repair_after not in ("sent", "queued"):
            raise ValueError(f"FECMOQ_REPAIR_AFTER={self.repair_after!r}; expected sent or queued")
        self.sent_wait_s: dict[int, float] = {}
        self.sent_timeouts = 0

    def block(self, b: int) -> list[bytes]:
        return make_block(self.seed, b, self.n_objects, self.gop)

    async def slot(self, b: int) -> None:
        if self.t_start is None:
            self.t_start = time.monotonic()
        delay = self.t_start + b * self.gap_s - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)

    def _event(self, b: int) -> asyncio.Event:
        return self._src_out.setdefault(b, asyncio.Event())

    async def release_after_source(self, b: int, out) -> None:
        """Wait as the repair policy says, then release block b's repair."""
        await _drained(out)
        if self.repair_after == "sent":
            waited, timed_out = await _source_sent(out, self.gap_s)
            self.sent_wait_s[b] = waited
            self.sent_timeouts += timed_out
        self.source_out(b)

    def source_out(self, b: int) -> None:
        self.last_out = max(self.last_out, b)
        self._event(b).set()
        self._src_out.pop(b - self.KEEP, None)

    async def wait_source_out(self, b: int) -> None:
        if self.has_source:
            if b > self.last_out:
                await self._event(b).wait()
        else:
            await self.slot(b)

    # -- incremental encoding: called by the source track as it writes the block --------
    def begin_block(self, b: int) -> list[bytes]:
        """The block's objects; with incremental encoding, also starts its encoder."""
        objs = self.block(b)
        if self.encode_mode == "incremental":
            sizes = [len(o) for o in objs]
            lay = layout_block(sizes, SPAN)
            slices = rung_slices(lay.k, self.rung_fracs)
            self._inc[b] = dict(sizes=sizes, lay=lay, slices=slices, syms=to_symbols(lay, objs),
                                enc=IsalIncrementalEncoder(lay.k, slices[-1].stop, lay.symbol_size),
                                fed=0, t=0.0)
        return objs

    def _feed_upto(self, b: int, n: int) -> None:
        st = self._inc.get(b)
        if st is None:
            return
        t = time.perf_counter()
        while st["fed"] < min(n, st["lay"].k):
            st["enc"].add(st["fed"], st["syms"][st["fed"]])
            st["fed"] += 1
        st["t"] += time.perf_counter() - t

    def object_written(self, b: int, i: int) -> None:
        """Source object i of block b is out: feed every symbol it completed."""
        st = self._inc.get(b)
        if st is not None:
            lay = st["lay"]
            self._feed_upto(b, (lay.object_starts[i] + lay.object_sizes[i]) // lay.symbol_size)

    def symbol_written(self, b: int, j: int) -> None:
        """Source symbol j of block b is out (datagram source)."""
        self._feed_upto(b, j + 1)

    def finish_block(self, b: int) -> None:
        """All of block b's source is written: complete the parity."""
        st = self._inc.pop(b, None)
        if st is None:
            return
        t = time.perf_counter()
        self._feed_upto_state(st)
        repair = st["enc"].repair()
        crit = time.perf_counter() - t
        self.encode_s[b] = crit
        self.encode_total_s[b] = st["t"] + crit
        self._encoded[b] = (st["sizes"], st["slices"], repair)
        self._encoded.pop(b - self.KEEP, None)

    @staticmethod
    def _feed_upto_state(st) -> None:
        while st["fed"] < st["lay"].k:
            st["enc"].add(st["fed"], st["syms"][st["fed"]])
            st["fed"] += 1

    def repair(self, b: int):
        """(sizes, slices, repair symbols) for block b, encoded once for all rungs."""
        if b not in self._encoded:
            objs = self.block(b)
            sizes = [len(o) for o in objs]
            lay = layout_block(sizes, SPAN)
            slices = rung_slices(lay.k, self.rung_fracs)
            t = time.perf_counter()
            symbols = Coder(lay.k, slices[-1].stop, self.coder).encode(to_symbols(lay, objs))
            self.encode_s[b] = self.encode_total_s[b] = time.perf_counter() - t
            self._encoded[b] = (sizes, slices, symbols)
            self._encoded.pop(b - self.KEEP, None)
        return self._encoded[b]


def _object_of_symbol(lay, j: int) -> int:
    """The object in which source symbol j starts (spread emission writes a symbol at its
    object's time)."""
    return max(0, bisect.bisect_right(lay.object_starts, j * lay.symbol_size) - 1)


class SourceTrack(PublishedTrack):
    mapping = StreamMapping.PER_GROUP

    def __init__(self, session, *, seed, n_blocks, n_objects, gap_s, schedule=None):
        # fetch_history_bytes=0: FETCH is answered from the schedule (FetchServer), so aiomoqt's
        # per-track copy of what was sent (0.12+) would be dead weight
        super().__init__(session, NAMESPACE, SOURCE_TRACK, fetch_history_bytes=0)
        self.seed, self.n_blocks, self.n_objects, self.gap_s = seed, n_blocks, n_objects, gap_s
        self.schedule = schedule or BlockSchedule(seed=seed, n_blocks=n_blocks,
                                                  n_objects=n_objects, gap_s=gap_s)
        self.schedule.has_source = True

    # Fraction of the block period over which the block's objects are spread: object i
    # is written at slot(b) + i * spread * gap_s / n_objects. 0 = all at once (default).
    spread = 0.0

    async def _spread_wait(self, b: int, i: int) -> None:
        if self.spread > 0 and i > 0:
            sch = self.schedule
            delay = (sch.t_start + b * self.gap_s + i * self.spread * self.gap_s / self.n_objects
                     - time.monotonic())
            if delay > 0:
                await asyncio.sleep(delay)

    async def produce(self, out):
        sch = self.schedule
        for b in range(self.n_blocks):
            await sch.slot(b)
            for i, obj in enumerate(sch.begin_block(b)):
                await self._spread_wait(b, i)
                await out.write(b, i, obj, group_start=(i == 0))
                sch.object_written(b, i)
            sch.finish_block(b)
            await sch.release_after_source(b, out)


def fetch_objects(sch, rung, g0: int, o0: int, g1: int, o1: int) -> list[tuple[int, int, bytes]]:
    """Repair by FETCH, the publisher's side: [(group, object, payload)] of the source track (rung
    None) or of rung `rung` (1-based) from Location (g0, o0) to (g1, o1), as a standalone FETCH
    names them -- the End Object is one past the last one wanted, and 0 means the whole of group g1.
    Payloads are what the track's own publisher sends (DatagramSourceTrack, RepairTrack with global
    ids); only blocks already produced (sch.last_out) are served, recomputed if they have left the
    encoder's cache. Ascending by (group, object).

    relay_fec no longer serves FETCH with this (since 2026-10-24: FetchServer, below, which also
    covers stream source, per-rung ids, one-symbol groups and END_OF_GROUP)."""
    bpg = getattr(sch, "blocks_per_group", 1)
    out = []
    for g in range(g0, g1 + 1):
        for q in range(bpg):
            b = g * bpg + q
            if b > sch.last_out or b >= sch.n_blocks:
                continue
            if rung is None:
                blk = sch.block(b)
                sizes = [len(x) for x in blk]
                payload = {j: pack_repair(sizes, sym)
                           for j, sym in enumerate(to_symbols(layout_block(sizes, SPAN), blk))}
            else:
                sizes, slices, repair = sch.repair(b)
                payload = {j: pack_repair(sizes, repair[j]) for j in slices[rung - 1]}
            for j, p in payload.items():
                gg, o, _ = sch.address(b, j)
                if (gg, o) < (g0, o0) or (gg == g1 and o1 and o >= o1):
                    continue
                out.append((gg, o, p))
    return sorted(out)


# Several blocks per group (BlockSchedule.address): block q of a group uses Objects
# q * OBJECT_STRIDE .. q * OBJECT_STRIDE + OBJECT_STRIDE - 1.
OBJECT_STRIDE = 256

# --repair-group-per-symbol: repair symbol j of block b is group b * GROUP_STRIDE + j (object 0).
# j < GROUP_STRIDE always (a block's repair symbols are well under 256).
GROUP_STRIDE = 256


class RepairTrack(PublishedTrack):
    """One rung of the protection ladder. object_id = GLOBAL repair index."""
    mapping = StreamMapping.DATAGRAM

    def __init__(self, session, *, seed, n_blocks, n_objects, gap_s, rung, rung_fracs,
                 schedule=None):
        super().__init__(session, NAMESPACE, repair_track_name(rung), fetch_history_bytes=0)
        self.seed, self.n_blocks, self.n_objects, self.gap_s = seed, n_blocks, n_objects, gap_s
        self.rung, self.rung_fracs = rung, list(rung_fracs)
        self.schedule = schedule or BlockSchedule(seed=seed, n_blocks=n_blocks, n_objects=n_objects,
                                                  gap_s=gap_s, rung_fracs=rung_fracs)

    async def produce(self, out):
        sch = self.schedule
        # A rung first subscribed mid-run (a subscriber adding protection when it sees
        # loss) starts at the latest block whose source is out, not at block 0: repair
        # for blocks long past is useless and would leave as one burst.
        b0 = max(0, sch.last_out) if sch.t_start is not None else 0
        for b in range(b0, self.n_blocks):
            await sch.wait_source_out(b)
            sizes, slices, repair = sch.repair(b)
            mine = slices[self.rung - 1]
            last = len(mine) - 1
            t_rel = time.monotonic()
            for n, j in enumerate(mine):
                if self.pace and n:
                    # --pace-repair: this rung's symbols spread evenly over pace x the block
                    # period after the block's source is out, instead of back to back
                    delay = t_rel + n * self.pace * self.gap_s / len(mine) - time.monotonic()
                    if delay > 0:
                        await asyncio.sleep(delay)
                ext = EOG_MARK if (self.end_of_group and n == last) else None
                if self.group_per_symbol:
                    # every repair symbol its own one-object group (relays that keep only
                    # object 0 of a datagram group); the global index rides in the payload
                    await out.write(b * GROUP_STRIDE + j, 0, pack_repair(sizes, repair[j], index=j),
                                    group_start=True)
                elif self.per_rung_ids:
                    await out.write(b, n, pack_repair(sizes, repair[j], index=j),
                                    extensions=ext, group_start=(n == 0))
                else:
                    g, o, first = sch.repair_address(b, n, j)
                    await out.write(g, o, pack_repair(sizes, repair[j]),
                                    extensions=ext, group_start=first)
            self._last_written = b

    catch_up = False     # relay_fec --repair-catch-up
    caught_up = 0        # blocks replayed to a re-subscribed peer (catch_up)
    end_of_group = False  # relay_fec --repair-eog: last object END_OF_GROUP bit + EOG status datagram
    per_rung_ids = False  # relay_fec --repair-ids per-rung: object ids 0..n-1, index in payload
    group_per_symbol = False  # relay_fec --repair-group-per-symbol: group b*GROUP_STRIDE + j, object 0
    pace = 0.0  # relay_fec --pace-repair FRAC: spread a block's repair over FRAC of the block period

    def _add_delivery(self, sub) -> None:
        # aiomoqt runs produce() once per track, however subscribers come and go, and a
        # peer attaching to it joins at the next group. A rung RE-subscribed mid-run
        # (live adaptation) therefore misses the block in flight -- its repair has just
        # gone out to no one -- where a first subscription, which starts produce() at
        # that block, gets it. With catch_up, replay the last block written to the new
        # peer's lane only (datagram rungs: a lane mid-stream needs no subgroup state).
        # (aiomoqt before 0.12 called this hook _attach.)
        super()._add_delivery(sub)
        if self.end_of_group and self.mapping == StreamMapping.DATAGRAM:
            # aiomoqt's delivery, swapped for one that can set the datagram's
            # end-of-group bit (EogDatagramDelivery)
            _swap_delivery(self, sub, EogDatagramDelivery(sub.session, sub.track_alias,
                                                          priority=self.priority,
                                                          mapping=self.mapping))
        b = getattr(self, "_last_written", None)
        if not self.catch_up or b is None or self.mapping != StreamMapping.DATAGRAM:
            return
        sizes, slices, repair = self.schedule.repair(b)
        lane = self._out.lanes[-1]
        mine = slices[self.rung - 1]
        born = time.monotonic()
        self.caught_up += 1
        for n, j in enumerate(mine):
            ext = EOG_MARK if (self.end_of_group and n == len(mine) - 1) else None
            if self.per_rung_ids:
                lane.offer(lane_item(b, n, pack_repair(sizes, repair[j], index=j), ext, n == 0, born))
            else:
                g, o, _ = self.schedule.address(b, j)
                lane.offer(lane_item(g, o, pack_repair(sizes, repair[j]), ext, n == 0, born))


def lane_item(group_id, object_id, payload, extensions, group_start, born) -> tuple:
    """One item for an aiomoqt fan-out lane, as FanoutDelivery.write queues it (0.13.2+:
    group, object, payload, extensions, group start, birth time, shared encoding cache).
    tests/test_aiomoqt_compat.py checks the shape against aiomoqt's own."""
    return (group_id, object_id, payload, extensions, group_start, born, None)


def _swap_delivery(track, sub, delivery) -> None:
    """Replace the delivery aiomoqt's PublishedTrack._add_delivery just gave a peer with
    our own, keeping the peer's filter, Forward State gate and delivery deadline."""
    old = sub.delivery
    delivery.passes, delivery.excludes_end = old.passes, old.excludes_end
    track._out.drop_session(sub.session)
    sub.delivery = delivery
    track._out.add(delivery, gate=lambda: sub.forward and sub.generating,
                   deadline=lambda: track._delivery_deadline(sub))


class ObjectSourceTrack(SourceTrack):
    """Milestone 4 arm C: source objects unchanged and still reliable, but one uni
    stream per object (aiomoqt's PER_OBJECT: subgroup id = object id, FIN after the
    object) instead of one per group. A lost packet holds back only its own object."""
    mapping = StreamMapping.PER_OBJECT


class DatagramSourceTrack(SourceTrack):
    """Milestone 4 arm B: the source block as symbols, one symbol per object, one
    object per datagram. object_id = source symbol index, so a lost datagram erases
    exactly one symbol. Each payload carries the block's object sizes in the repair
    wire format, so the receiver can rebuild the layout from any one symbol. Nothing
    is fragmented: every object fits one datagram (wire.DATAGRAM_MAX_PAYLOAD)."""
    mapping = StreamMapping.DATAGRAM

    # relay_fec --pace-source FRAC: spread the symbols that start in one source object evenly
    # over FRAC of that object's interval (spread x the block period / n_objects), counted from
    # its first symbol's write, instead of back to back. With spread 0 the whole block is one
    # interval (FRAC of the block period). 0 = back to back (the default).
    pace = 0.0

    async def produce(self, out):
        sch = self.schedule
        for b in range(self.n_blocks):
            await sch.slot(b)
            objs = sch.begin_block(b)
            sizes = [len(o) for o in objs]
            lay = layout_block(sizes, SPAN)
            owner = [_object_of_symbol(lay, j) for j in range(lay.k)]
            if self.pace and self.spread > 0:
                key, span = owner, self.spread * self.gap_s / self.n_objects
            else:
                key, span = [0] * lay.k, self.gap_s
            count = {i: key.count(i) for i in set(key)}
            t0, m = None, 0
            for j, sym in enumerate(to_symbols(lay, objs)):
                await self._spread_wait(b, owner[j])
                if self.pace:
                    if j == 0 or key[j] != key[j - 1]:
                        t0, m = time.monotonic(), 0
                    else:
                        m += 1
                        delay = t0 + m * self.pace * span / count[key[j]] - time.monotonic()
                        if delay > 0:
                            await asyncio.sleep(delay)
                g, o, first = sch.address(b, j)
                await out.write(g, o, pack_repair(sizes, sym), group_start=first)
                sch.symbol_written(b, j)
            sch.finish_block(b)
            await sch.release_after_source(b, out)


class SymbolStreamSourceTrack(DatagramSourceTrack):
    """Arm D: arm B's source symbols (one symbol per object, object_id = symbol
    index, block sizes carried in every payload) on arm C's mapping -- one uni
    stream per object. Reliable, so a lost symbol is retransmitted, but a lost
    packet holds back only its own symbol, and repair can stand in for it."""
    mapping = StreamMapping.PER_OBJECT


class ObjectRepairTrack(RepairTrack):
    """Arm D: a repair rung on one uni stream per object instead of datagrams.
    Reliable like the symbols; a lost repair symbol is retransmitted too."""
    mapping = StreamMapping.PER_OBJECT


class SymbolMirrorTrack(DatagramSourceTrack):
    """A second source family offered alongside the first in the same run: the same
    blocks, cut into symbols, published under another track name. It reads each
    block from the schedule (deterministic from the seed) at the block's slot and
    writes its symbols, but does not drive the schedule -- the primary source track
    feeds the encoder and gates repair -- so both families come from one encode.
    mapping is set per instance (datagram for arm B, per object for arm D)."""

    def __init__(self, session, *, mapping, **kw):
        super().__init__(session, **kw)
        self.mapping = mapping
        self.schedule.has_source = True

    async def produce(self, out):
        sch = self.schedule
        for b in range(self.n_blocks):
            await sch.slot(b)
            objs = sch.block(b)
            sizes = [len(o) for o in objs]
            lay = layout_block(sizes, SPAN)
            for j, sym in enumerate(to_symbols(lay, objs)):
                await self._spread_wait(b, _object_of_symbol(lay, j))
                await out.write(b, j, pack_repair(sizes, sym), group_start=(j == 0))


# ---- arm D2: one stream per source object's symbols, one per rung per block --------

from bisect import bisect_right  # noqa: E402

from aiomoqt.delivery import SubgroupDelivery  # noqa: E402
from aiomoqt.messages import SubgroupHeader  # noqa: E402


class SubgroupByKeyDelivery(SubgroupDelivery):
    """A delivery that puts each object on the stream of the subgroup subgroup_of()
    names: a new uni stream (with that subgroup id) whenever the subgroup changes,
    FIN on the previous one. Several objects share a stream, unlike PER_OBJECT, and a
    group has several streams, unlike PER_GROUP (which always uses subgroup 0). Built
    on aiomoqt's SubgroupDelivery from the outside; aiomoqt itself is unchanged."""

    def __init__(self, session, track_alias, *, priority=128, subgroup_of):
        super().__init__(session, track_alias, priority=priority,
                         mapping=StreamMapping.PER_GROUP)
        self._subgroup_of = subgroup_of
        self._cur = None

    def _close_subgroup(self) -> None:
        if self._stream_id is not None:
            self.session.stream_write(self._stream_id, b"", end_stream=True)
            self._stream_id = None
            self._header = None

    async def write(self, group_id, object_id, payload, *, extensions=None, group_start=False,
                    encoded=None):
        # encoded: aiomoqt 0.13.2+'s shared per-object encoding cache; this delivery
        # encodes per stream itself.
        if callable(extensions):
            extensions = extensions(self.session)
        sg = self._subgroup_of(group_id, object_id)
        if self._stream_id is None or self._cur != (group_id, sg):
            self._close_subgroup()
            self._stream_id = await self.session.open_uni_stream()
            self.stream_count += 1
            self._header = SubgroupHeader(
                track_alias=self.track_alias, group_id=group_id, subgroup_id=sg,
                publisher_priority=self.priority, extensions_present=True, prof=self._prof)
            self.session.stream_write(self._stream_id, self._header.serialize().data)
            self._cur = (group_id, sg)
        buf = self._header.next_object(payload=payload, extensions=extensions,
                                       object_id=object_id)
        await self.session.stream_write_drain(self._stream_id, buf.data)
        self._note_largest(group_id, object_id)
        self.objects_sent += 1
        self.bytes_sent += len(payload)
        return True


class SymbolSubgroupSourceTrack(DatagramSourceTrack):
    """Arm D2: arm B's source symbols, the symbols of each source object on one
    stream (subgroup id = that object's index). A symbol belongs to the object its
    first byte belongs to, so ~one stream per source object (12 a block, fewer when
    a small object fits inside a symbol that another object starts)."""
    mapping = StreamMapping.PER_GROUP          # what it is closest to, for logs

    def _subgroup_of(self, b, j):
        starts = self._starts.get(b)
        if starts is None:
            sizes = [len(o) for o in self.schedule.block(b)]
            starts = layout_block(sizes, SPAN).object_starts
            self._starts[b] = starts
            self._starts.pop(b - 16, None)
        from .symbols import SYMBOL_SIZE
        return bisect_right(starts, j * SYMBOL_SIZE) - 1

    def _add_delivery(self, sub) -> None:
        if not hasattr(self, "_starts"):
            self._starts = {}
        super()._add_delivery(sub)
        _swap_delivery(self, sub, SubgroupByKeyDelivery(sub.session, sub.track_alias,
                                                        priority=self.priority,
                                                        subgroup_of=self._subgroup_of))


class GroupRepairTrack(RepairTrack):
    """Arm D2: a rung's symbols for a block on one stream (aiomoqt's PER_GROUP)."""
    mapping = StreamMapping.PER_GROUP


# --- END_OF_GROUP on datagram tracks (relay_fec --repair-eog) ----------------------------
# A datagram track otherwise carries no end of group, so a relay cache serving a FETCH of
# a whole group can't tell that the last object it holds is the last: moqx asks upstream
# for the "missing tail" and, when the publisher can't serve FETCH, resets the fetch
# stream after delivering everything. aiomoqt 0.11.1's SubgroupDelivery never sets
# ObjectDatagram.end_of_group (type bit 0x02); this delivery does, for an object written
# with EOG_MARK as its extensions, AND follows it with an END_OF_GROUP status datagram at
# the next object id. The bit alone isn't enough: moqx 55d68d5's cache ignores a
# datagram's lastInGroup (it only forwards it) and learns a group's end only from an
# END_OF_GROUP status object, which marks every later id as nonexistent. The mark rides
# aiomoqt's fan-out lanes unchanged and is stripped here, so nothing of it reaches the
# wire. aiomoqt itself is unchanged.
EOG_MARK = {"fecmoq-end-of-group": True}


class EogDatagramDelivery(SubgroupDelivery):
    async def write(self, group_id, object_id, payload, *, extensions=None, group_start=False,
                    encoded=None):
        if extensions is EOG_MARK or (isinstance(extensions, dict) and "fecmoq-end-of-group" in extensions):
            from aiomoqt.messages.data import ObjectDatagram
            dgram = ObjectDatagram(track_alias=self.track_alias, group_id=group_id,
                                   object_id=object_id, publisher_priority=self.priority,
                                   extensions=None, payload=payload, end_of_group=True)
            await self.session.dgram_write_drain(dgram.serialize(prof=self._prof))
            from aiomoqt.types import ObjectStatus
            eog = ObjectDatagram(track_alias=self.track_alias, group_id=group_id,
                                 object_id=object_id + 1, publisher_priority=self.priority,
                                 status=ObjectStatus.END_OF_GROUP, payload=b"")
            await self.session.dgram_write_drain(eog.serialize(prof=self._prof))
            self._note_largest(group_id, object_id)
            self.objects_sent += 1
            self.bytes_sent += len(payload)
            return True
        return await super().write(group_id, object_id, payload, extensions=extensions,
                                   group_start=group_start, encoded=encoded)


# --- FETCH answered by the publisher (relay_fec --serve-fetch, the default) ---------------
# Every object our tracks send is a pure function of the block schedule (the seed, or the
# memoised media), so a FETCH is answered by recomputing what the live track wrote: the same
# group and object ids, subgroups, payloads and END_OF_GROUP status objects, for any block in
# the recovery window, and for a rung nobody subscribed to (the relay sends that FETCH upstream).
# aiomoqt's PublishedTrack keeps a byte-bounded copy of what it sent instead (0.12+); a
# ScheduleHistory stands in for that copy, so aiomoqt's own FETCH code resolves the range,
# joining FETCH and the End of Unknown Range marker for what has left the window.

from dataclasses import dataclass  # noqa: E402


@dataclass(frozen=True)
class FetchSpec:
    """What one track writes, as a FETCH has to repeat it."""
    kind: str                         # source | mirror | repair
    source_mapping: str = "group"     # source: group | object | datagram | symbol-object | symbol-subgroup
    mapping: StreamMapping = StreamMapping.DATAGRAM   # mirror and repair
    rung: int = 0
    per_rung_ids: bool = False
    group_per_symbol: bool = False
    end_of_group: bool = False


def track_objects(sch, spec: FetchSpec, b: int) -> list[tuple]:
    """[(group, object, subgroup or None for a datagram, payload, status or None)] that the
    live track writes for block b, END_OF_GROUP status objects included: the datagram one
    --repair-eog sends after a rung's last object, and the one aiomoqt (0.12+) closes a
    PER_GROUP stream with, at the next object id."""
    from aiomoqt.types import ObjectStatus
    eog = ObjectStatus.END_OF_GROUP
    out = []
    if spec.kind == "source" and spec.source_mapping in ("group", "object"):
        objs = sch.block(b)
        for i, obj in enumerate(objs):
            out.append((b, i, 0 if spec.source_mapping == "group" else i, obj, None))
        if spec.source_mapping == "group":
            out.append((b, len(objs), 0, b"", eog))
        return out
    if spec.kind in ("source", "mirror"):
        objs = sch.block(b)
        sizes = [len(o) for o in objs]
        lay = layout_block(sizes, SPAN)
        for j, sym in enumerate(to_symbols(lay, objs)):
            if spec.kind == "mirror":
                g, o = b, j
                sg = None if spec.mapping == StreamMapping.DATAGRAM else o
            else:
                g, o, _ = sch.address(b, j)
                sg = {"datagram": None, "symbol-object": o,
                      "symbol-subgroup": _object_of_symbol(lay, j)}[spec.source_mapping]
            out.append((g, o, sg, pack_repair(sizes, sym), None))
        return out
    sizes, slices, repair = sch.repair(b)
    mine = slices[spec.rung - 1]
    last = len(mine) - 1
    for n, j in enumerate(mine):
        if spec.group_per_symbol:
            out.append((b * GROUP_STRIDE + j, 0, None, pack_repair(sizes, repair[j], index=j), None))
            continue
        if spec.per_rung_ids:
            g, o, p = b, n, pack_repair(sizes, repair[j], index=j)
        else:
            (g, o, _), p = sch.repair_address(b, n, j), pack_repair(sizes, repair[j])
        sg = {StreamMapping.DATAGRAM: None, StreamMapping.PER_OBJECT: o,
              StreamMapping.PER_GROUP: 0}[spec.mapping]
        out.append((g, o, sg, p, None))
        if n == last and ((spec.end_of_group and spec.mapping == StreamMapping.DATAGRAM)
                          or spec.mapping == StreamMapping.PER_GROUP):
            out.append((g, o + 1, sg, b"", eog))
    return out


class ScheduleHistory:
    """aiomoqt's ObjectHistory interface (fetch, last_object, evicted_through), answered
    from the schedule. It holds the last `window_blocks` blocks whose source is out; a FETCH
    reaching further back gets FETCH_OK with an End of Unknown Range marker for the part
    that has left, as aiomoqt's own history answers (not a refusal)."""

    def __init__(self, sch, spec: FetchSpec, window_blocks: int, priority: int = 128):
        self.sch, self.spec, self.window, self.priority = sch, spec, window_blocks, priority
        # No END_OF_GROUP status objects in an answer: from draft 16 a FETCH object has no
        # Object Status (d16 §10.2.1.1, d18 §11.2.1.1). aiomoqt 0.13.3+ skips them at d16+
        # and its receivers reject them; moqx fdba9db closes the session on one.
        self.last = None                  # (objects, markers, first, last) of the last fetch()

    def _span(self) -> tuple[int, int]:
        """(oldest, newest) block held; newest < oldest when none is."""
        hi = min(self.sch.last_out, self.sch.n_blocks - 1)
        return max(0, hi - self.window + 1), hi

    def _groups(self, b: int) -> tuple[int, int]:
        """The group ids block b's objects use, inclusive."""
        if self.spec.kind == "repair" and self.spec.group_per_symbol:
            return b * GROUP_STRIDE, b * GROUP_STRIDE + GROUP_STRIDE - 1
        g = b // getattr(self.sch, "blocks_per_group", 1)
        return g, g

    def _group_last(self, g: int) -> int:
        """The largest object id the track writes in group g (0 if none)."""
        if self.spec.kind == "repair" and self.spec.group_per_symbol:
            blocks = [g // GROUP_STRIDE]
        else:
            bpg = getattr(self.sch, "blocks_per_group", 1)
            blocks = range(g * bpg, g * bpg + bpg)
        ids = [o for b in blocks for gg, o, *_ in track_objects(self.sch, self.spec, b) if gg == g]
        return max(ids, default=0)

    def _block_last(self, b: int) -> tuple[int, int]:
        return max((g, o) for g, o, *_ in track_objects(self.sch, self.spec, b))

    @property
    def evicted_through(self):
        lo, hi = self._span()
        return self._block_last(lo - 1) if hi >= lo and lo > 0 else None

    def largest(self):
        """Largest Location of the data objects held, None before any block is out."""
        lo, hi = self._span()
        if hi < lo:
            return None
        return max((g, o) for g, o, _, _, st in track_objects(self.sch, self.spec, hi) if st is None)

    def _held(self, start, end) -> list[tuple]:
        lo, hi = self._span()
        out = []
        for b in range(lo, hi + 1):
            g0, g1 = self._groups(b)
            if g1 < start[0] or g0 > end[0]:
                continue
            out += [r for r in track_objects(self.sch, self.spec, b) if start <= (r[0], r[1]) <= end]
        return sorted(out, key=lambda r: (r[0], r[1]))

    def last_object(self, group_id: int):
        ids = [o for g, o, _, _, st in self._held((group_id, 0), (group_id, 1 << 62)) if st is None]
        return max(ids) if ids else None

    def fetch(self, start, end, session=None, *, descending=False, lost_marker=None):
        """FetchObjects from start to end, both (group, object) and inclusive, as aiomoqt's
        ObjectHistory.fetch returns them (groups ascending or descending, objects ascending,
        the dropped part as one End of Range marker at the low end)."""
        from aiomoqt.messages.data import FETCH_FLAGS_END_UNKNOWN, FetchObject
        from aiomoqt.types import ObjectStatus
        lost = None
        through = self.evicted_through
        if through is not None and start <= through:
            lost = min(through, end)
            if lost[1] >= (1 << 62) - 1:
                # A whole End Group (aiomoqt resolves End Object 0 to object 1 << 62) that has
                # left the window: end the marker at that group's real last object. Taken
                # as is, 1 << 62 overflows the varint and the FETCH fails after FETCH_OK
                # (aiomoqt 0.13.1's ObjectHistory does exactly that).
                lost = (lost[0], self._group_last(lost[0]))
            start = max(start, (lost[0], lost[1] + 1))
        recs = self._held(start, end)
        if descending:
            recs.sort(key=lambda r: (-r[0], r[1]))
        recs = [r for r in recs if r[4] is None]
        out = [FetchObject(group_id=g, subgroup_id=sg or 0, object_id=o,
                           publisher_priority=self.priority, payload=p,
                           status=ObjectStatus.NORMAL if st is None else st,
                           datagram=sg is None)
               for g, o, sg, p, st in recs]
        if lost is not None:
            marker = FetchObject(group_id=lost[0], object_id=lost[1],
                                 end_of_range=lost_marker or FETCH_FLAGS_END_UNKNOWN)
            out.insert(len(out) if descending else 0, marker)
        self.last = (len(recs), int(lost is not None), start, end)
        return out


class FetchServer(PublishedTrack):
    """Answers FETCH for one track name with aiomoqt's PublishedTrack FETCH code (range,
    joining, End of Unknown Range, serving the stream), its history a ScheduleHistory. It
    never produces: the live track, if any, is a separate object."""

    def __init__(self, session, name: str, history: ScheduleHistory):
        super().__init__(session, NAMESPACE, name, priority=history.priority, fetch_history_bytes=0)
        self._history = history

    @property
    def _largest(self):
        return self._history.largest()

    @_largest.setter
    def _largest(self, value):            # PublishedTrack.__init__ resets it; the schedule rules
        pass
