"""MPEG-TS over MoQ with FEC: the TS split into objects and groups, and back.

Layout, per draft-gregoire-moq-msfts-02 (MSF packaging "mpeg2ts"):

- An object is a run of whole 188-byte TS packets, unchanged: no header, no extension
  (msfts 5.1). We put PACKETS_PER_OBJECT (7, 1,316 bytes: the usual TS-over-UDP payload)
  in each, fewer only in the object just before a random access point.
- A new group starts at each random access point (msfts 5.2): a packet on the video PID
  (the PCR PID when the PMT names no video stream) whose adaptation field has
  random_access_indicator set. That packet is the first one of the group's first object.
  Group 0 starts at the first packet of the stream and holds the first random access point
  too (a file starts PAT, PMT, IDR), unless drop_until_rap (a live input joined
  mid-stream), when everything before the first random access point is dropped.
- Object ids in a group are 0, 1, 2, ... with no holes, so a receiver that only follows
  msfts (moq2ts's counts a hole as a gap) reads the source track as is.

FEC (ours, not msfts: it rides separate repair tracks). A group's objects are cut into
FEC blocks of block_objects consecutive objects (the last block of a group may be
shorter). Block q of group g holds source objects q*n .. q*n+k-1, and its repair symbols are
Objects q * OBJECT_STRIDE + j of group g on the repair tracks (j = global repair index), the
addressing fecmoq.moq already uses for several blocks per group.

Timing: the TS carries its own (PCR); nothing is added. A file is paced by PCR, a live
source by its own arrival.
"""
from __future__ import annotations

import json
import socket
import time
from dataclasses import dataclass, field

TS_PACKET = 188
SYNC = 0x47
PACKETS_PER_OBJECT = 7
OBJECT_BYTES = TS_PACKET * PACKETS_PER_OBJECT
NULL_PID = 0x1FFF
# PAT/PMT copies at group starts (TsPacketizer psi_each_group): none, packets added, or packets
# put in place of null packets
PSI_MODES = ("off", "insert", "replace")
# replace: how many packets after a group start a null packet may be taken from
PSI_NULL_WINDOW = 100
PCR_HZ = 27_000_000
VIDEO_STREAM_TYPES = {0x01, 0x02, 0x10, 0x1B, 0x24, 0x42, 0xD1, 0xEA}


def pid_of(pkt) -> int:
    return ((pkt[1] & 0x1F) << 8) | pkt[2]


def random_access(pkt) -> bool:
    """random_access_indicator set in the packet's adaptation field."""
    afc = (pkt[3] >> 4) & 0x3
    return afc in (2, 3) and pkt[4] > 0 and bool(pkt[5] & 0x40)


def pcr_of(pkt) -> int | None:
    """The packet's PCR in 27 MHz units, or None."""
    afc = (pkt[3] >> 4) & 0x3
    if afc not in (2, 3) or pkt[4] < 7 or not pkt[5] & 0x10:
        return None
    b = pkt[6:12]
    base = (b[0] << 25) | (b[1] << 17) | (b[2] << 9) | (b[3] << 1) | (b[4] >> 7)
    ext = ((b[4] & 1) << 8) | b[5]
    return base * 300 + ext


class PsiTracker:
    """Just enough PAT/PMT parsing to find the video PID and the PCR PID of the first
    program (single-section tables that start in one packet, as muxers write them)."""

    def __init__(self):
        self.pmt_pid = None
        self.video_pid = None
        self.pcr_pid = None
        self.scte35_pids: list[int] = []         # stream type 0x86, in PMT order

    @staticmethod
    def _section(pkt) -> bytes | None:
        if not pkt[1] & 0x40:                    # payload_unit_start_indicator
            return None
        afc = (pkt[3] >> 4) & 0x3
        off = 4
        if afc in (2, 3):
            off += 1 + pkt[4]
        if afc not in (1, 3) or off >= TS_PACKET:
            return None
        off += 1 + pkt[off]                      # pointer_field
        return bytes(pkt[off:])

    def feed(self, pkt) -> None:
        pid = pid_of(pkt)
        if pid == 0 and self.pmt_pid is None:
            s = self._section(pkt)
            if s and s[0] == 0x00:
                n = ((s[1] & 0x0F) << 8) | s[2]
                for i in range(8, 3 + n - 4, 4):
                    prog = (s[i] << 8) | s[i + 1]
                    if prog != 0:
                        self.pmt_pid = ((s[i + 2] & 0x1F) << 8) | s[i + 3]
                        break
        elif pid == self.pmt_pid and self.pcr_pid is None:
            s = self._section(pkt)
            if s and s[0] == 0x02:
                n = ((s[1] & 0x0F) << 8) | s[2]
                self.pcr_pid = ((s[8] & 0x1F) << 8) | s[9]
                i = 12 + (((s[10] & 0x0F) << 8) | s[11])
                end = 3 + n - 4
                while i + 5 <= end:
                    st, epid = s[i], ((s[i + 1] & 0x1F) << 8) | s[i + 2]
                    if st in VIDEO_STREAM_TYPES and self.video_pid is None:
                        self.video_pid = epid
                    elif st == 0x86:
                        self.scte35_pids.append(epid)
                    i += 5 + (((s[i + 3] & 0x0F) << 8) | s[i + 4])

    @property
    def rap_pid(self):
        return self.video_pid if self.video_pid is not None else self.pcr_pid


@dataclass
class TsObject:
    data: bytes
    group_start: bool          # first object of a group (a random access point)
    pcr_first: int | None = None   # PCR (27 MHz) of the object's first PCR packet, if any


class TsPacketizer:
    """Incremental TS -> objects. feed() bytes as they come; it returns the objects
    completed so far (PACKETS_PER_OBJECT packets, or fewer when a random access point
    follows). flush() returns the last, partial one.

    psi_each_group: "insert" starts every group after the first with copies of the last PAT
    and PMT packets (msfts: a group SHOULD start with them, so a joiner at a group can decode
    at once). "replace" puts each copy in place of the first null packet after the group
    start instead, so the packet count, the rate and every PCR's position stay the input's; a
    copy that finds no null packet within null_window packets, or whose PID sends a fresh
    original first, is skipped (psi_skipped, and `log` if given). Each copy is a legal
    duplicate packet (same continuity_counter, the next packet of its PID after its
    original), so the program is otherwise unchanged; tables that do not fit in one packet
    are not copied. Each original is copied at most once (only one duplicate is legal), so a
    group starting before the next PAT and PMT gets no copies. True means "insert"."""

    def __init__(self, per_object: int = PACKETS_PER_OBJECT, drop_until_rap: bool = False,
                 psi_each_group=False, null_window: int = PSI_NULL_WINDOW, log=None):
        self.per_object = per_object
        self.drop_until_rap = drop_until_rap
        mode = {True: "insert", False: "off", None: "off"}.get(psi_each_group, psi_each_group)
        if mode not in PSI_MODES:
            raise ValueError(f"psi_each_group {psi_each_group!r}: one of {PSI_MODES}")
        self.psi_mode = mode
        self.psi_each_group = mode != "off"
        self.null_window = null_window
        self.log = log
        self._pat = self._pmt = None
        self._pending: list[bytes] = []      # replace: copies waiting for a null packet
        self._pending_age = 0                # packets since the group start that queued them
        self.psi_inserted = 0                # copies placed, either mode
        self.psi_skipped = 0                 # replace: copies with no null packet in reach
        self.psi_superseded = 0              # replace: copies whose PID sent a fresh original first
        self.psi_null_offsets: list[int] = []   # replace: packets from the group start to each copy
        self.groups = 0
        self.psi = PsiTracker()
        self._rest = b""
        self._cur: list[bytes] = []
        self._cur_start = False
        self._cur_pcr = None
        self.started = not drop_until_rap
        self.first = True
        self.seen_rap = False
        self.dropped_packets = 0
        self.packets = 0

    def _close(self, out: list) -> None:
        if self._cur:
            out.append(TsObject(b"".join(self._cur), self._cur_start, self._cur_pcr))
        self._cur, self._cur_start, self._cur_pcr = [], False, None

    def feed(self, data: bytes) -> list[TsObject]:
        buf = self._rest + bytes(data)
        out: list[TsObject] = []
        i = 0
        while i + TS_PACKET <= len(buf):
            if buf[i] != SYNC:
                # resync: TS from a file or UDP is packet aligned; anything else is garbage
                j = buf.find(bytes([SYNC]), i + 1)
                if j < 0:
                    i = len(buf)
                    break
                self.dropped_packets += 1
                i = j
                continue
            pkt = buf[i:i + TS_PACKET]
            i += TS_PACKET
            self.packets += 1
            self.psi.feed(pkt)
            if self.psi_each_group:
                self._keep_psi(pkt)
            rap = self.psi.rap_pid is not None and pid_of(pkt) == self.psi.rap_pid and random_access(pkt)
            if not self.started:
                if not rap:
                    self.dropped_packets += 1
                    continue
                self.started = True
            if self.first:
                # group 0 starts at the first packet kept: the stream start (whose PAT/PMT
                # then lead it), or the first random access point (drop_until_rap)
                self._close(out)
                self._cur_start = True
                self.first = False
                self.seen_rap = rap
            elif rap and not self.seen_rap:
                self.seen_rap = True             # the first RAP stays in group 0, after PAT/PMT
            elif rap:
                self._close(out)
                self._cur_start = True
                self.groups += 1
                if self._pending:
                    self._skip(f"the next group started {self._pending_age} packets after")
                if self.psi_each_group and self._pat is not None and self._pmt is not None:
                    if self.psi_mode == "insert":
                        self._cur += [self._pat, self._pmt]
                        self.psi_inserted += 2
                    else:
                        self._pending, self._pending_age = [self._pat, self._pmt], 0
                    # ISO/IEC 13818-1 2.4.3.3 permits ONE duplicate of a packet: a group that
                    # starts before the next PAT and PMT (a short GOP at a scene cut) gets none
                    self._pat = self._pmt = None
            if self._pending:
                pkt = self._replace_null(pkt)
            if self._cur_pcr is None and pid_of(pkt) == self.psi.pcr_pid:
                self._cur_pcr = pcr_of(pkt)
            self._cur.append(pkt)
            if len(self._cur) == self.per_object:
                self._close(out)
        self._rest = buf[i:]
        return out

    def _replace_null(self, pkt):
        """replace: the packet to send in `pkt`'s place, a waiting copy where it is null."""
        pid = pid_of(pkt)
        if pid == NULL_PID:
            copy = self._pending.pop(0)
            self.psi_inserted += 1
            self.psi_null_offsets.append(self._pending_age)
            self._pending_age += 1
            return copy
        for c in self._pending:
            if pid_of(c) == pid:                 # a fresh original: its copy would no longer follow it
                self._pending.remove(c)
                self.psi_superseded += 1
                break
        self._pending_age += 1
        if self._pending and self._pending_age > self.null_window:
            self._skip(f"no null packet in the {self.null_window} packets after")
        return pkt

    def _skip(self, why: str) -> None:
        self.psi_skipped += len(self._pending)
        if self.log is not None:
            pids = "+".join("PAT" if pid_of(c) == 0 else "PMT" for c in self._pending)
            self.log(f"TS-PSI-SKIP group {self.groups}: {pids} copy not sent, {why} the group start")
        self._pending = []

    def _keep_psi(self, pkt) -> None:
        pid = pid_of(pkt)
        if pid != 0 and pid != self.psi.pmt_pid:
            return
        if pid == 0:
            self._pat = None                     # a copy must directly follow its original
        else:
            self._pmt = None
        s = PsiTracker._section(pkt)
        if not s or len(s) < 3 or 3 + (((s[1] & 0x0F) << 8) | s[2]) > len(s):
            return                               # not a section start, or one spanning packets
        if pid == 0:
            self._pat = bytes(pkt)
        else:
            self._pmt = bytes(pkt)

    def flush(self) -> list[TsObject]:
        out: list[TsObject] = []
        self._close(out)
        return out


def split_ts(data: bytes, per_object: int = PACKETS_PER_OBJECT,
             drop_until_rap: bool = False, psi_each_group=False) -> list[TsObject]:
    p = TsPacketizer(per_object, drop_until_rap, psi_each_group)
    return p.feed(data) + p.flush()


def psi_each_group_default(src: str) -> str:
    """Whether groups start with PAT/PMT copies when the publisher isn't told (--ts-psi-each-group
    or --no-ts-psi-each-group). Off where the TS is carried unmodified, so the output stays
    byte-identical to the input: a file, read as is, and a GPAC URL source, passed through. On
    where the publisher writes the mux itself, so there are no input bytes to keep: avgen
    (encoded and muxed in-process). The copies are msfts's SHOULD: a joiner at a group start
    decodes at once instead of waiting for the next PAT/PMT. Returns a PSI_MODES value."""
    return "insert" if src == "avgen" else "off"


def object_times(objs: list[TsObject]) -> list[float]:
    """Seconds from the first object for each object, from PCR: linear between the objects
    that carry one, extrapolated at the ends at the mean rate. PCR wrap is ignored (a
    26.5-hour period; a test file never wraps unless the muxer starts near it)."""
    pts = [(i, o.pcr_first) for i, o in enumerate(objs) if o.pcr_first is not None]
    if len(pts) < 2:
        return [0.0] * len(objs)
    (i0, p0), (i1, p1) = pts[0], pts[-1]
    rate = (p1 - p0) / PCR_HZ / (i1 - i0)               # seconds per object, mean
    out = [0.0] * len(objs)
    k = 0
    for i in range(len(objs)):
        while k + 1 < len(pts) and pts[k + 1][0] <= i:
            k += 1
        ia, pa = pts[k]
        if k + 1 < len(pts) and i >= ia:
            ib, pb = pts[k + 1]
            t = (pa - p0) / PCR_HZ + (i - ia) * (pb - pa) / PCR_HZ / (ib - ia)
        else:
            t = (pa - p0) / PCR_HZ + (i - ia) * rate
        out[i] = t
    t0 = out[0]
    return [t - t0 for t in out]


PCR_WRAP = (1 << 33) * 300


def ts_file_info(path: str, span: int = 188 * 20_000) -> dict:
    """A TS file's PIDs (PsiTracker over its first span bytes) and its mean rate: bytes from
    the first PCR in its head to the last in its tail over the PCRs' difference (b/s, None
    without PCRs on the PCR PID). Reads only the head and the tail."""
    import os
    size = os.path.getsize(path)
    psi = PsiTracker()
    with open(path, "rb") as f:
        head = f.read(span)
        f.seek(max(0, (size - span) // TS_PACKET * TS_PACKET))
        tail_off = f.tell()
        tail = f.read()
    for i in range(0, len(head) - TS_PACKET + 1, TS_PACKET):
        psi.feed(head[i:i + TS_PACKET])

    def pcrs(buf, off):
        for i in range(0, len(buf) - TS_PACKET + 1, TS_PACKET):
            pkt = buf[i:i + TS_PACKET]
            if pkt[0] == SYNC and pid_of(pkt) == psi.pcr_pid:
                p = pcr_of(pkt)
                if p is not None:
                    yield off + i, p
    first = next(pcrs(head, 0), None)
    last = None
    for last in pcrs(tail, tail_off):
        pass
    rate = None
    if first and last and last[0] > first[0]:
        dt = (last[1] - first[1]) % PCR_WRAP / PCR_HZ
        rate = (last[0] - first[0]) * 8 / dt if dt > 0 else None
    return dict(size=size, video_pid=psi.video_pid, pcr_pid=psi.pcr_pid, rate_bps=rate,
                scte35_pids=psi.scte35_pids)


class PcrClock:
    """object_times() for a TS read a piece at a time, so a long file never has to be in
    memory: push() objects as they come; it returns (object, t) for those whose time is now
    known, t in seconds from the first object. Objects between two that carry a PCR are
    timed linearly between them; the ones before the first PCR, once the second is in, at
    the rate between the first two; the ones after the last (flush()), at the mean rate so
    far. A PCR wrap is unwrapped; a PCR discontinuity (discontinuity_indicator) is not
    handled, and a backwards PCR is timed as if the stream kept its mean rate."""

    def __init__(self):
        self.pending: list[TsObject] = []    # objects since the last PCR-carrying one
        self.lead: list[TsObject] = []       # objects before the first PCR
        self.n = 0                           # objects timed or pending, in order
        self.last = None                     # (object index, unwrapped PCR) of the last PCR object
        self.first = None                    # the same, of the first
        self.t0 = None                       # time of object 0, s on the PCR clock
        self.wrap = 0
        self.last_t = 0.0                    # the latest time handed out

    def _rate(self) -> float | None:
        """Mean seconds per object between the first and last PCR so far."""
        if self.first is None or self.last is None or self.last[0] == self.first[0]:
            return None
        return (self.last[1] - self.first[1]) / PCR_HZ / (self.last[0] - self.first[0])

    def push(self, obj: TsObject) -> list[tuple[TsObject, float]]:
        i = self.n
        self.n += 1
        if obj.pcr_first is None:
            (self.pending if self.first is not None else self.lead).append(obj)
            return []
        p = obj.pcr_first + self.wrap
        if self.last is not None and p < self.last[1] - PCR_WRAP // 2:
            self.wrap += PCR_WRAP                  # the 33-bit base wrapped
            p += PCR_WRAP
        out = []
        if self.first is None:
            self.first = self.last = (i, p)
            self.lead.append(obj)                  # timed once a second PCR gives a rate
            return out
        ia, pa = self.last
        if p <= pa:                                # backwards: keep the mean rate
            p = pa + (i - ia) * (self._rate() or 0.0) * PCR_HZ
        if self.lead:
            # the objects before the first PCR (and that one), at the first interval's rate
            per = (p - pa) / PCR_HZ / (i - ia)
            i0, p0 = self.first
            self.t0 = p0 / PCR_HZ - i0 * per
            for k, o in enumerate(self.lead):
                out.append((o, k * per))
            self.lead = []
        for k, o in enumerate(self.pending, 1):
            out.append((o, (pa + (p - pa) * k / (i - ia)) / PCR_HZ - self.t0))
        self.pending = []
        self.last = (i, p)
        out.append((obj, p / PCR_HZ - self.t0))
        self.last_t = out[-1][1]
        return out

    def flush(self) -> list[tuple[TsObject, float]]:
        """The rest, at the mean rate so far (or all at 0 with fewer than two PCRs)."""
        rate = self._rate()
        if rate is None:
            objs = self.lead + self.pending
            self.lead, self.pending = [], []
            return [(o, 0.0) for o in objs]
        ia, pa = self.last
        out = [(o, (pa / PCR_HZ - self.t0) + k * rate) for k, o in enumerate(self.pending, 1)]
        self.pending = []
        if out:
            self.last_t = out[-1][1]
        return out


@dataclass
class TsBlock:
    group: int
    q: int                         # block index in its group
    first_object: int              # object id of its first object in the group
    objects: list = field(default_factory=list)
    times: list = field(default_factory=list)       # per object, s since the source start
    closed: bool = False
    ends_group: bool = False
    index: int = 0                 # position in TsBlocks.blocks


class TsBlocks:
    """Objects -> FEC blocks: block_objects consecutive objects of one group per block.
    add() an object as it comes; a block closes when full or when the next group starts
    (or at end()). Returns the blocks that closed, in order."""

    def __init__(self, block_objects: int):
        if not 1 <= block_objects <= 255:
            raise ValueError("block_objects must be 1..255 (the repair header counts them in a u8)")
        self.n = block_objects
        self.blocks: list[TsBlock] = []
        self.group = -1
        self.next_object = 0

    def add(self, obj: TsObject, t: float) -> tuple[int, int, list[TsBlock]]:
        """(block index, object id in the group, blocks closed by this object)."""
        closed = []
        cur = self.blocks[-1] if self.blocks else None
        if obj.group_start or cur is None:
            if cur is not None and not cur.closed:
                cur.closed = cur.ends_group = True
                closed.append(cur)
            self.group += 1
            self.next_object = 0
            cur = TsBlock(self.group, 0, 0, index=len(self.blocks))
            self.blocks.append(cur)
        elif cur.closed:
            cur = TsBlock(self.group, cur.q + 1, self.next_object, index=len(self.blocks))
            self.blocks.append(cur)
        cur.objects.append(obj.data)
        cur.times.append(t)
        oid = self.next_object
        self.next_object += 1
        if len(cur.objects) == self.n:
            cur.closed = True
            closed.append(cur)
        return len(self.blocks) - 1, oid, closed

    def end(self) -> list[TsBlock]:
        cur = self.blocks[-1] if self.blocks else None
        if cur is not None and not cur.closed:
            cur.closed = cur.ends_group = True
            return [cur]
        if cur is not None:
            cur.ends_group = True
        return []


def catalog_json(*, source: str, rungs: list[float], block_objects: int, symbol_size: int,
                 mux_rate: int | None, pcr_pid: int | None, object_stride: int,
                 namespace: str | None = None, scte35_pids=()) -> bytes:
    """MSF catalog with the source as an msfts track (packaging "mpeg2ts", msfts 6 and 7.1)
    and the repair rungs under an unregistered packaging, so a player without the FEC stage
    skips them."""
    rung_names = [f"rep{r}" for r in range(1, len(rungs) + 1)]
    src = {"name": source, "packaging": "mpeg2ts", "mpeg2tsMode": "unmodified-program",
           "mpeg2tsPacketSize": TS_PACKET, "mpeg2tsRandomAccess": True, "isLive": True,
           "role": "video", "mimeType": "video/mp2t",
           "x-fec": {"scheme": "rs-cauchy-gf256", "symbolSize": symbol_size,
                     "blockObjects": block_objects, "objectStride": object_stride,
                     "rungs": rung_names, "rungFractions": list(rungs)}}
    if namespace:
        src["namespace"] = namespace
    if mux_rate:
        # MSF bitrate only: msfts-02 6.7 says mpeg2tsMuxRate MUST be absent in the unmodified
        # modes (the stream keeps its null packets and carries its own rate)
        src["bitrate"] = int(mux_rate)
    if pcr_pid is not None:
        src["mpeg2tsPcrPid"] = pcr_pid
    if scte35_pids:
        # msfts-02 6.11: SHOULD be present when the track carries SCTE-35. It holds one PID;
        # a program with several (one per splice feed) gets its first, and the PMT lists all
        src["mpeg2tsScte35Pid"] = scte35_pids[0]
    tracks = [src] + [{"name": n, "packaging": "x-fec-repair", "isLive": True,
                       "x-fec": {"source": source, "rung": r}}
                      for r, n in enumerate(rung_names, 1)]
    return json.dumps({"version": 1, "generatedAt": int(time.time() * 1000),
                       "tracks": tracks}).encode()


# ------------------------------------------------------------------ receiver side

@dataclass
class _RxBlock:
    src: dict = field(default_factory=dict)      # object id in group -> (payload, how)
    rep: dict = field(default_factory=dict)      # global repair index -> repair payload
    sizes: list | None = None
    t_first: float | None = None
    fetch_at: float | None = None                # when the FETCH for its holes went out
    emitted: int = 0                             # objects 0..emitted-1 already written out


class TsReassembler:
    """MoQ source objects + repair symbols -> the TS byte stream, in (group, object) order.

    Each block (g, q) is released, in order, as soon as it is complete, or decodable from
    its repair symbols. Otherwise it waits `window` s after the first arrival of any later
    block (the publisher has moved past it); then, if `fetch`
    is set, the missing objects are requested (want_fetch()) and the block waits up to
    `fetch_wait` s more for them. What is still missing then is skipped, without filler,
    and logged (group, object). So no output byte waits longer than about
    window + fetch_wait after the next block starts arriving.

    How many objects a block has comes from its repair header (every repair symbol carries
    the block's object sizes); how many a group has, from its END_OF_GROUP (a stream per
    group closes with one; END_OF_TRACK for the last). A shorter block ends its group. When
    none of these says, a group is taken to have ended group_end_grace s after a later group
    started with nothing of its next block in sight (so a group whose last block lost every
    source object AND every repair symbol is not detectable as lost)."""

    def __init__(self, block_objects: int, *, window: float = 0.5, fetch: bool = False,
                 fetch_wait: float = 0.5, stride: int = 256, codec: str | None = None,
                 group_end_grace: float = 0.05):
        self.n, self.window, self.fetch, self.fetch_wait = block_objects, window, fetch, fetch_wait
        # A group's last block can be full (block_objects objects), and a stream per object
        # carries no END_OF_GROUP: once a later group has started and nothing of the next block
        # of this group has come for this long, the group is taken to have ended.
        self.group_end_grace = min(window, group_end_grace)
        self.stride, self.codec = stride, codec
        self.blocks: dict[tuple[int, int], _RxBlock] = {}
        self.cursor: tuple[int, int] | None = None
        self.newest_first: dict[tuple[int, int], float] = {}
        self.stats = dict(objects_out=0, direct=0, fec=0, fetched=0, lost=0, late=0,
                          blocks_out=0, blocks_decoded=0, blocks_with_loss=0, fetch_requests=0,
                          groups_skipped=0, bytes_out=0)
        self.lost: list[tuple[int, int]] = []    # (group, object) skipped
        self._fetch_q: list[tuple[int, int, int]] = []   # (group, first object, end object excl.)
        self.max_hold_s = 0.0
        self.group_len: dict[int, int] = {}      # group -> object count, from END_OF_GROUP
        self.group_end_t: dict[int, float] = {}  # group -> when its END_OF_GROUP came
        self.holds: list[tuple[int, int, float]] = []   # (group, block, s from first arrival to release)
        self.waits: list[float] = []             # per object written: s from its arrival to its write
        # Objects of a group past the last block released for it are lost too, but nothing
        # said so when the cursor moved on (its END_OF_GROUP and its tail came after the
        # group-end grace). They are counted when the tail or the END_OF_GROUP turns up.
        self.group_hi: dict[int, int] = {}       # group -> object ids released or counted, below
        self.start_group: int | None = None
        self._tail_lost: set[tuple[int, int]] = set()

    # -- input -------------------------------------------------------------
    def _blk(self, key, t) -> _RxBlock:
        b = self.blocks.get(key)
        if b is None:
            b = self.blocks[key] = _RxBlock()
        if b.t_first is None:
            b.t_first = t
        return b

    def _done(self, key) -> bool:
        return self.cursor is not None and key < self.cursor

    def add_source(self, group: int, obj: int, payload: bytes, t: float, how: str = "direct"):
        key = (group, obj // self.n)
        if self._done(key):
            self.stats["late"] += 1
            self._count_tail(group, obj + 1)
            return
        if self.cursor is None and obj // self.n == 0:
            self.cursor = key
            self.start_group = group
        b = self._blk(key, t)
        b.src.setdefault(obj % self.n, (bytes(payload), how, t))

    def add_repair(self, group: int, obj: int, payload: bytes, t: float):
        from .wire import unpack_repair
        key = (group, obj // self.stride)
        if self._done(key):
            return
        b = self._blk(key, t)
        if b.sizes is None:
            try:
                b.sizes = unpack_repair(bytes(payload), expect=self.codec)[0]
            except Exception:                    # noqa: BLE001  not ours / wrong coder
                return
        b.rep[obj % self.stride] = bytes(payload)

    def end_of_group(self, group: int, end_object: int, t: float | None = None) -> None:
        """An END_OF_GROUP status at object id end_object: the group has end_object objects."""
        self.group_len[group] = end_object
        if t is not None:
            self.group_end_t.setdefault(group, t)
        if self.cursor is not None and self.cursor[0] > group:
            self._count_tail(group, end_object)

    def _count_tail(self, group: int, upto: int) -> None:
        """Objects below upto of a group the cursor has left, never released: lost."""
        if self.start_group is None or group < self.start_group:
            return                               # before this subscriber's first group
        for o in range(self.group_hi.get(group, 0), upto):
            if (group, o) not in self._tail_lost:
                self._tail_lost.add((group, o))
                self.lost.append((group, o))
                self.stats["lost"] += 1
                self.stats["lost_after_group_end"] = self.stats.get("lost_after_group_end", 0) + 1

    def track_end(self, group: int, obj: int, track_ended: bool = False) -> None:
        """After the final poll: the track reached (group, obj) (the relay's Largest Location,
        from TRACK_STATUS during the run), but nothing past what was released ever arrived
        here (a relay that stopped sending to this subscriber). Count what is missing:
        exactly where a group's length is known, otherwise an estimate from the mean length
        of the groups written (stats lost_unseen_est). Nothing to do once the output reached
        the end (END_OF_TRACK, or the last group released through obj)."""
        if self.start_group is None or not self.group_hi:
            return
        if track_ended and obj == 0:
            # the publisher ended the track: (group, 0) is its END_OF_TRACK status (whose
            # datagram may not have reached us), an empty group, not an object
            self.group_len.setdefault(group, 0)
        last = max(self.group_hi)
        # the Largest Location can be the group's END_OF_GROUP status (at its object count)
        if group < last or (group == last and self.group_hi[last] >= self.group_len.get(group, obj + 1)):
            return
        known = [n for g, n in self.group_len.items() if g in self.group_hi and g < last]
        mean = sum(known) / len(known) if known else None
        exact = est = unknown = 0
        for g in range(last, group + 1):
            have = self.group_hi.get(g, 0)
            n = self.group_len.get(g)
            if g == group and n is None:
                n = obj + 1
            if n is not None:
                for o in range(have, n):
                    if (g, o) not in self._tail_lost:
                        self._tail_lost.add((g, o))
                        self.lost.append((g, o))
                        exact += 1
            elif mean is not None:
                est += max(0, round(mean) - have)
            else:
                unknown += 1
        if not (exact or est or unknown):
            return                               # e.g. the END_OF_TRACK group (length 0)
        self.stats["lost"] += exact
        self.stats["lost_after_output_end"] = exact
        self.stats["lost_unseen_est"] = est
        self.stats["groups_unseen_unknown_length"] = unknown
        self.stats["truncated_at"] = [last, self.group_hi[last]]
        self.stats["track_end"] = [group, obj]

    def want_fetch(self) -> list[tuple[int, int, int]]:
        q, self._fetch_q = self._fetch_q, []
        return q

    # -- output ------------------------------------------------------------
    def _anchor(self, key) -> float | None:
        """When block key's wait starts: the first arrival of any LATER block, i.e. once the
        publisher has moved past it. (From the block's own first arrival, a window shorter
        than a block's duration expired blocks whose tail had not been sent yet.)"""
        later = [bb.t_first for k, bb in self.blocks.items() if k > key and bb.t_first is not None]
        if later:
            return min(later)
        # Nothing later yet: the group's END_OF_GROUP says the publisher is past its last
        # block (the track's last block has no later block, and would wait for ever)
        g, q = key
        glen = self.group_len.get(g)
        if glen is not None and (q + 1) * self.n >= glen:
            return self.group_end_t.get(g)
        return None

    def _k(self, b: _RxBlock) -> int | None:
        return len(b.sizes) if b.sizes is not None else None

    def _decode(self, b: _RxBlock) -> list[bytes] | None:
        from .codec import Coder
        from .symbols import SPAN, erased_symbols, from_symbols, layout_block, to_symbols
        from .wire import repair_codec, unpack_repair
        k_obj = len(b.sizes)
        lay = layout_block(b.sizes, SPAN)
        present = {i: x[0] for i, x in b.src.items() if i < k_obj}
        missing = [i for i in range(k_obj) if i not in present]
        objs = [present.get(i, bytes(b.sizes[i])) for i in range(k_obj)]
        syms = to_symbols(lay, objs)
        avail = {j: s for j, s in enumerate(syms) if j not in erased_symbols(lay, missing)}
        codec = repair_codec(next(iter(b.rep.values())))
        for j, p in b.rep.items():
            avail[lay.k + j] = unpack_repair(p, expect=codec)[1]
        if len(avail) < lay.k:
            return None
        out = Coder(lay.k, 0, codec).decode(avail)
        return None if out is None else from_symbols(lay, out)

    def _emit(self, item, now: float, emit) -> None:
        data, how, t = item
        self.stats[how] += 1
        self.stats["objects_out"] += 1
        self.stats["bytes_out"] += len(data)
        self.waits.append(now - t)
        emit(data)

    def _stream_prefix(self, b: _RxBlock, now: float, emit) -> None:
        """Write the block's objects that are next in order and here: no waiting for the rest
        of the block when nothing before them is missing."""
        while b.emitted in b.src:
            self._emit(b.src[b.emitted], now, emit)
            b.emitted += 1

    def _release(self, key, objs: list, now: float, emit) -> None:
        """Finish block key: objs[i] is (data, how, t) or None (lost) for each object i;
        the ones already streamed are skipped."""
        b = self.blocks.pop(key, None)
        start = b.emitted if b is not None else 0
        if b is not None and b.t_first is not None:
            self.max_hold_s = max(self.max_hold_s, now - b.t_first)
            self.holds.append((key[0], key[1], now - b.t_first))
        lost_here = 0
        for i, item in enumerate(objs):
            if i < start:
                continue
            if item is None:
                self.lost.append((key[0], key[1] * self.n + i))
                self.stats["lost"] += 1
                lost_here += 1
                continue
            self._emit(item, now, emit)
        self.stats["blocks_out"] += 1
        self.stats["blocks_with_loss"] += bool(lost_here)
        g = key[0]
        self.group_hi[g] = max(self.group_hi.get(g, 0), key[1] * self.n + len(objs))

    def _advance(self, key, short: bool) -> None:
        g, q = key
        self.cursor = (g + 1, 0) if short else (g, q + 1)

    def _next_in_group(self, key) -> bool:
        return any(kk[0] == key[0] and kk[1] > key[1] for kk in self.blocks)

    def _fetch_expired(self, now: float) -> None:
        """FETCH the holes of every block whose window has run out, not just the one at the
        cursor: otherwise each block waits its own fetch_wait in turn, and a stretch of lost
        blocks (a group whose source never came) holds the output back by fetch_wait per block."""
        if not self.fetch or self.cursor is None:
            return
        keys = sorted(k for k in self.blocks if k >= self.cursor)
        later_first = None
        for key in reversed(keys):
            b = self.blocks[key]
            anchor = later_first if later_first is not None else self._anchor(key)
            if b.t_first is not None:
                later_first = b.t_first if later_first is None else min(later_first, b.t_first)
            if b.fetch_at is not None or anchor is None or now - anchor < self.window:
                continue
            g, q = key
            k = self._k(b)
            glen = self.group_len.get(g)
            if k is None and glen is not None:
                k = min(self.n, glen - q * self.n)
            n_obj = k if k is not None else (
                self.n if self._next_in_group(key) else max(b.src, default=-1) + 1)
            holes = [i for i in range(n_obj) if i not in b.src]
            if holes:
                b.fetch_at = now
                self._fetch_q.append((g, q * self.n + min(holes), q * self.n + max(holes) + 1))
                self.stats["fetch_requests"] += 1

    def poll(self, now: float, emit, final: bool = False) -> None:
        """Release every block that is ready; emit(bytes) is called per object, in order."""
        if not final:
            self._fetch_expired(now)
        while self.cursor is not None:
            key = self.cursor
            g, q = key
            b = self.blocks.get(key)
            glen = self.group_len.get(g)
            if glen is not None and q * self.n >= glen:
                self.blocks.pop(key, None)
                self.cursor = (g + 1, 0)
                continue
            if b is None:
                anchor = self._anchor(key)
                wait = self.window if self._next_in_group(key) else self.group_end_grace
                if anchor is None or not (final or now - anchor >= wait):
                    if final and anchor is None:
                        self.cursor = None
                    if not final or anchor is None:
                        return
                if self._next_in_group(key):
                    # nothing of it came, but a later block of its group did: lost whole
                    b = self.blocks[key] = _RxBlock(t_first=anchor)
                else:
                    # the group ended at q - 1 (or, for q == 0, group g never came)
                    nxt = min(kk for kk in self.blocks if kk > key)
                    self.stats["groups_skipped"] += nxt[0] - g - (0 if q == 0 else 1)
                    self.cursor = (nxt[0], 0)
                    continue
            self._stream_prefix(b, now, emit)
            k = self._k(b)
            if k is None and glen is not None:
                k = min(self.n, glen - q * self.n)
            n_obj = k if k is not None else (
                self.n if self._next_in_group(key) else max(b.src, default=-1) + 1)
            short = (k is not None and k < self.n) or (glen is not None and (q + 1) * self.n >= glen)
            have = sum(1 for i in b.src if i < n_obj)
            if n_obj and have == n_obj and (k is not None or n_obj == self.n):
                self._release(key, [b.src[i] for i in range(n_obj)], now, emit)
                self._advance(key, short)
                continue
            if k is not None and b.rep:
                dec = self._decode(b)
                if dec is not None:
                    objs = [b.src[i] if i in b.src else (dec[i], "fec", b.t_first) for i in range(k)]
                    self.stats["blocks_decoded"] += 1
                    self._release(key, objs, now, emit)
                    self._advance(key, short)
                    continue
            anchor = self._anchor(key)
            if not (final or (anchor is not None and now - anchor >= self.window)):
                return
            holes = [i for i in range(n_obj) if i not in b.src]
            if self.fetch and holes and not final:
                if b.fetch_at is None:
                    b.fetch_at = now
                    self._fetch_q.append((g, q * self.n + min(holes), q * self.n + max(holes) + 1))
                    self.stats["fetch_requests"] += 1
                    return
                if now - b.fetch_at < self.fetch_wait:
                    return
            self._release(key, [b.src.get(i) for i in range(n_obj)], now, emit)
            self._advance(key, short)


class TsOutput:
    """The reassembled TS to a file, stdout ('-') or udp://host:port (7 packets per
    datagram, the hand-off to a broadcast receiver)."""

    def __init__(self, dst: str):
        import sys
        self.dst = dst
        self.sock = None
        self.f = None
        self._pending = b""
        self.total = 0
        self.timeline: list[tuple[float, int]] = []   # (monotonic, bytes written so far) per write
        if dst.startswith("udp://"):
            host, port = dst[len("udp://"):].rsplit(":", 1)
            self.addr = (host, int(port))
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        elif dst == "-":
            self.f = sys.__stdout__.buffer          # relay_fec points sys.stdout at stderr then
        else:
            self.f = open(dst, "wb")

    def write(self, data: bytes) -> None:
        self.total += len(data)
        if len(self.timeline) < 100_000:
            self.timeline.append((time.monotonic(), self.total))
        if self.sock is None:
            self.f.write(data)
            return
        buf = self._pending + data
        n = len(buf) // OBJECT_BYTES * OBJECT_BYTES
        for i in range(0, n, OBJECT_BYTES):
            self.sock.sendto(buf[i:i + OBJECT_BYTES], self.addr)
        self._pending = buf[n:]

    def close(self) -> None:
        if self.sock is not None:
            if self._pending:
                self.sock.sendto(self._pending, self.addr)
            self.sock.close()
        elif self.f is not None:
            self.f.flush()
            if self.dst != "-":
                self.f.close()
