"""MPEG-TS publishing: the TS block schedule, its source track, and the GPAC source.

TsSchedule is a BlockSchedule whose blocks are the FEC blocks of fecmoq.ts.TsBlocks: they
appear as the TS comes in (a file read at once and paced by PCR, or a live GPAC session) and
close when full or when the next group starts. Repair for a block is encoded when it closes
(batch mode: the incremental encoder needs a block's object sizes up front). The repair
tracks (fecmoq.moq.RepairTrack) and FETCH serving (ScheduleHistory) run on it unchanged
through address()/repair_address()/group_of()/blocks_of_group().

GPAC runs in its own thread (gpac_ts_reader) and hands TS bytes to the event loop with
call_soon_threadsafe, so it never blocks the asyncio publisher.
"""
from __future__ import annotations

import asyncio
import os
import sys
import threading
import time

from aiomoqt.delivery import StreamMapping

from .moq import (EOG_ONLY, OBJECT_STRIDE, BlockSchedule, EogDatagramDelivery, SourceTrack,
                  _source_sent, _swap_delivery)
from .ts import PcrClock, TsBlocks, TsPacketizer, object_times


class TsSchedule(BlockSchedule):
    def __init__(self, *, block_objects: int, rung_fracs, nominal_rate_bps: float,
                 pace: bool, drop_until_rap: bool = False, psi_each_group=False,
                 seed: int = 0, packets_per_object: int = 7, psi_null_window: int | None = None,
                 log=None):
        # gap_s: a block's nominal duration; it bounds the wait for a block's source to leave
        # the send buffer (release_after_source) and sizes the FETCH window in blocks
        from .symbols import SYMBOL_SIZE
        from .wire import DATAGRAM_MAX_PAYLOAD
        # every repair datagram carries the block's object sizes (fecmoq.wire: 2 + 2 n bytes)
        if 2 + 2 * block_objects + SYMBOL_SIZE > DATAGRAM_MAX_PAYLOAD:
            raise ValueError(f"{block_objects} objects per block: a repair object would be "
                             f"{2 + 2 * block_objects + SYMBOL_SIZE} B, over the "
                             f"{DATAGRAM_MAX_PAYLOAD} B datagram ceiling; at most "
                             f"{(DATAGRAM_MAX_PAYLOAD - SYMBOL_SIZE - 2) // 2} with "
                             f"{SYMBOL_SIZE} B symbols")
        gap_s = block_objects * 188 * packets_per_object * 8 / nominal_rate_bps
        super().__init__(seed=seed, n_blocks=1 << 30, n_objects=block_objects, gap_s=gap_s,
                         rung_fracs=rung_fracs)
        self.encode_mode = "batch"
        self.tb = TsBlocks(block_objects)
        kw = {} if psi_null_window is None else {"null_window": psi_null_window}
        self.packetizer = TsPacketizer(packets_per_object, drop_until_rap=drop_until_rap,
                                       psi_each_group=psi_each_group, log=log, **kw)
        self.pace = pace
        self.events: asyncio.Queue = asyncio.Queue()
        self.ended = asyncio.Event()
        self.objects_in = 0
        self.bytes_in = 0
        self.t_src0 = None          # monotonic time of the first live byte
        self.evicted = 0            # blocks [0, evicted) have dropped their objects (feed_file)
        self.evicted_reads = 0      # block() calls for one of them
        self.sha256 = None          # of the file's bytes (feed_file)

    # -- input (on the event loop) ----------------------------------------------
    def _add(self, obj, t: float) -> None:
        b, oid, closed = self.tb.add(obj, t)
        self.objects_in += 1
        self.events.put_nowait(("obj", b, oid, obj.data, t))
        for blk in closed:
            self.events.put_nowait(("close", blk.index))

    def load_file_objects(self, objs) -> None:
        """A whole file at once: object times from PCR, so the source track paces it."""
        for obj, t in zip(objs, object_times(objs)):
            self._add(obj, t)
        self.end()

    async def feed_file(self, path: str, *, lead_s: float = 2.0, keep_s: float = 30.0,
                        chunk: int = 188 * 1024, start: int = 0, length: int | None = None) -> None:
        """A file read a piece at a time, so a long one never has to be in memory: objects
        timed from PCR (PcrClock), read no more than lead_s ahead of the paced send position
        (before publishing starts, the first lead_s). Blocks whose objects are all more than
        keep_s behind it drop their objects (evict_before): a FETCH or a repair recompute for
        one then finds nothing. Also hashes the bytes read (self.sha256). start/length: only
        that byte range of the file (whole TS packets)."""
        import hashlib
        h = hashlib.sha256()
        clock = PcrClock()
        left = length
        with open(path, "rb") as f:
            f.seek(start)
            while True:
                now_t = (time.monotonic() - self.t_start) if self.t_start is not None else 0.0
                if clock.last_t > now_t + lead_s:
                    self.evict_before(now_t - keep_s)
                    await asyncio.sleep(0.05)
                    continue
                d = f.read(chunk if left is None else min(chunk, left))
                if left is not None:
                    left -= len(d)
                if not d:
                    break
                h.update(d)
                self.bytes_in += len(d)
                for obj in self.packetizer.feed(d):
                    for o, t in clock.push(obj):
                        self._add(o, t)
        for obj in self.packetizer.flush():
            for o, t in clock.push(obj):
                self._add(o, t)
        for o, t in clock.flush():
            self._add(o, t)
        self.sha256 = h.hexdigest()
        for blk in self.tb.end():
            self.events.put_nowait(("close", blk.index))
        self.events.put_nowait(("end",))

    def evict_before(self, t: float) -> None:
        """Drop the objects of closed blocks whose last object is timed before t."""
        tb = self.tb
        while self.evicted < len(tb.blocks):
            blk = tb.blocks[self.evicted]
            if not blk.closed or not blk.times or blk.times[-1] >= t:
                break
            blk.objects = []
            self.evicted += 1

    def push_bytes(self, data: bytes) -> None:
        """Live: TS bytes as they arrive; object time = arrival."""
        now = time.monotonic()
        if self.t_src0 is None:
            self.t_src0 = now
        self.bytes_in += len(data)
        for obj in self.packetizer.feed(data):
            self._add(obj, now - self.t_src0)

    def end(self) -> None:
        for obj in self.packetizer.flush():
            self._add(obj, (time.monotonic() - self.t_src0) if self.t_src0 else 0.0)
        for blk in self.tb.end():
            self.events.put_nowait(("close", blk.index))
        self.events.put_nowait(("end",))

    # -- BlockSchedule ------------------------------------------------------------
    def block(self, b: int) -> list[bytes]:
        if b < self.evicted:
            self.evicted_reads += 1
        return self.tb.blocks[b].objects

    def address(self, b: int, j: int):
        blk = self.tb.blocks[b]
        o = blk.first_object + j
        return blk.group, o, o == 0

    def repair_address(self, b: int, n: int, j: int):
        blk = self.tb.blocks[b]
        return blk.group, blk.q * OBJECT_STRIDE + j, n == 0 and blk.q == 0

    def group_of(self, b: int) -> int:
        return self.tb.blocks[b].group

    def blocks_of_group(self, g: int) -> list[int]:
        return [blk.index for blk in self.tb.blocks if blk.group == g]

    def block_ends_group(self, b: int) -> bool:
        return self.tb.blocks[b].ends_group

    async def release_after_source(self, b: int, out) -> None:
        """BlockSchedule's release without its unbounded wait for every lane to drain: a TS
        source never pauses, so with a stream per object the lanes are rarely all empty and
        repair would stall (it did, after block 9 of the first object-mapping run). Only the
        bounded check that stream data has left the send buffer, capped at one block period."""
        if self.repair_after == "sent":
            waited, timed_out = await _source_sent(out, self.gap_s)
            self.sent_wait_s[b] = waited
            self.sent_timeouts += timed_out
        self.source_out(b)


class TsSourceTrack(SourceTrack):
    """The TS source track: each object as it comes (paced by PCR for a file), group =
    random access point, object ids 0.. in the group; repair for a block is released once
    its source is out.

    With datagram source (mapping DATAGRAM), each object is one object datagram, and each
    group is closed by an END_OF_GROUP status datagram once the next group has started
    (or the TS ended): moqx serves datagram objects from its cache to a FETCH only for a
    group closed that way, and it tells a subscriber
    how many objects the group had. A lost status datagram leaves a subscriber without the
    group's length, and a lost tail of the group then goes unnoticed, so each END_OF_GROUP
    goes out EOG_COPIES times, one block apart (a loss burst rarely spans a block period).
    A relay forwards or caches the same status object again; a subscriber reads it once."""

    EOG_COPIES = 3

    def _add_delivery(self, sub) -> None:
        super()._add_delivery(sub)
        if self.mapping == StreamMapping.DATAGRAM:
            _swap_delivery(self, sub, EogDatagramDelivery(sub.session, sub.track_alias,
                                                          priority=self.priority,
                                                          mapping=self.mapping))

    async def produce(self, out):
        sch = self.schedule
        if sch.t_start is None:
            sch.t_start = time.monotonic()
        pending = []
        eogs = []           # [group, status object id, copies left] (datagram source)

        async def send_eogs(all_=False):
            for e in list(eogs):
                await out.write(e[0], e[1], b"", extensions=EOG_ONLY)
                e[2] -= 1
                if e[2] <= 0 or all_:
                    eogs.remove(e)
        while True:
            ev = await sch.events.get()
            if ev[0] == "obj":
                _, b, oid, data, t = ev
                if sch.pace:
                    delay = sch.t_start + t - time.monotonic()
                    if delay > 0:
                        await asyncio.sleep(delay)
                await out.write(sch.tb.blocks[b].group, oid, data, group_start=(oid == 0))
            elif ev[0] == "close":
                b = ev[1]
                sch.finish_block(b)
                blk = sch.tb.blocks[b]
                if self.mapping == StreamMapping.DATAGRAM:
                    # the earlier groups' further copies, then this group's first
                    await send_eogs()
                    if blk.ends_group and blk.objects:
                        await out.write(blk.group, blk.first_object + len(blk.objects), b"",
                                        extensions=EOG_ONLY)
                        if self.EOG_COPIES > 1:
                            eogs.append([blk.group, blk.first_object + len(blk.objects),
                                         self.EOG_COPIES - 1])
                # off the object path: waiting for the send buffer must not hold back the
                # next objects
                pending.append(asyncio.ensure_future(sch.release_after_source(b, out)))
                pending = [p for p in pending if not p.done()]
            else:
                while eogs:
                    await asyncio.sleep(sch.gap_s)
                    await send_eogs()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                sch.ended.set()
                return


def _gpac():
    try:
        import libgpac  # noqa: F401
    except ImportError:
        for d in (os.environ.get("GPAC_PYTHON", ""), "/usr/share/gpac/python",
                  "/usr/local/share/gpac/python"):
            if d and os.path.isdir(os.path.join(d, "libgpac")):
                sys.path.insert(0, d)
                break
    import libgpac
    return libgpac


def gpac_version() -> str:
    gpac = _gpac()
    return str(getattr(gpac, "version", "unknown"))


def gpac_graph(src: str, *, mux_rate: int, video_bps: int, audio_bps: int, gop_s: float,
               live_dur: float, realtime: bool) -> list[str]:
    """The GPAC filters, in order, that end in TS bytes for our sink."""
    if src == "avgen":
        return [f"avgen:dur={live_dur:g}",
                f"ffenc:c=avc:b={video_bps}:fintra={gop_s:g}:g={round(25 * gop_s)}",
                f"ffenc:c=aac:b={audio_bps}",
                f"m2tsmx:rate={mux_rate}" + (":realtime=true" if realtime else "")]
    return [src]


def gpac_ts_child(src: str, on_data, on_end, **opts) -> threading.Thread:
    """gpac_ts_reader in a child Python process (still GPAC's Python bindings), its TS bytes
    read from the child's stdout in a thread here: the GPAC session, encoders included, never
    shares this process's GIL with the publisher's event loop."""
    import json
    import subprocess
    src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    code = ("import sys; sys.path.insert(0, %r); from fecmoq.ts_moq import _child_main; _child_main()"
            % src_dir)
    proc = subprocess.Popen([sys.executable, "-c", code, json.dumps(dict(src=src, **opts))],
                            stdout=subprocess.PIPE)

    def pump():
        try:
            while True:
                d = proc.stdout.read1(1 << 16)
                if not d:
                    break
                on_data(d)
        finally:
            proc.wait()
            on_end()

    th = threading.Thread(target=pump, name="gpac-child", daemon=True)
    th.start()
    return th


def _child_main():
    import json
    a = json.loads(sys.argv[1])
    src = a.pop("src")
    out = os.dup(1)
    os.dup2(2, 1)                 # anything GPAC prints goes to stderr; the TS to the pipe

    def write(d):
        mv = memoryview(d)
        while mv:
            mv = mv[os.write(out, mv):]
    gpac_ts_reader(src, write, lambda: None, **a).join()


def gpac_ts_reader(src: str, on_data, on_end, *, mux_rate: int = 6_000_000,
                   video_bps: int = 5_000_000, audio_bps: int = 128_000, gop_s: float = 1.0,
                   live_dur: float = 120.0, realtime: bool = True) -> threading.Thread:
    """Start a GPAC filter session in a thread: src ("avgen", a .ts file, or a GPAC URL such
    as udp://host:port) through the filters of gpac_graph() into a custom sink that calls
    on_data(bytes) per TS packet batch and on_end() at the end. Callbacks run in the GPAC
    thread: wrap them with loop.call_soon_threadsafe."""
    gpac = _gpac()

    def run():
        try:
            fs = gpac.FilterSession()

            class TsSink(gpac.FilterCustom):
                def __init__(self, sess):
                    gpac.FilterCustom.__init__(self, sess, "TsToMoq")
                    # File streams only. No FileExt cap: GPAC 2.2.1's libgpac.py fails on any
                    # string-typed cap (_make_prop shadows str); the sink is linked explicitly
                    # (set_source) to the TS muxer or the TS input instead.
                    self.push_cap("StreamType", "File", gpac.GF_CAPS_INPUT)

                def configure_pid(self, pid, is_remove):
                    if not is_remove and pid not in self.ipids:
                        pid.send_event(gpac.FilterEvent(gpac.GF_FEVT_PLAY))
                    return 0

                def process(self):
                    for pid in self.ipids:
                        while True:
                            pck = pid.get_packet()
                            if pck is None:
                                if pid.eos:
                                    self.done = True
                                break
                            d = pck.data
                            if d is not None:
                                on_data(d.tobytes() if hasattr(d, "tobytes")
                                        else __import__("ctypes").string_at(d, pck.size))
                            pid.drop_packet()
                    return 0

            filters = [fs.load_src(g) if i == 0 else fs.load(g)
                       for i, g in enumerate(gpac_graph(src, mux_rate=mux_rate, video_bps=video_bps,
                                                        audio_bps=audio_bps, gop_s=gop_s,
                                                        live_dur=live_dur, realtime=realtime))]
            sink = TsSink(fs)
            sink.set_source(filters[-1])
            fs.run()
        finally:
            on_end()

    th = threading.Thread(target=run, name="gpac", daemon=True)
    th.start()
    return th
