"""Source track + a ladder of repair tracks THROUGH A RELAY, over a real network.

Milestone 3. Same tracks, wire format and reconstruction as scripts/loopback_fec.py;
what changes is where the bytes travel and what "lost" means.

Three roles, one per process, on different hosts:

  pub      connects to the relay as a client, announces the namespace, and serves
           `src` and `rep1..repN` as the relay SUBSCRIBEs upstream for each -- one
           handler dispatching on track name, as loopback_fec.py's server does (see
           run_pub for why not one PUBLISH per track). Production is held behind a
           gate file so both subscribers are subscribed before block 0 is sent --
           see the note on the gate below.
  sub      SUBSCRIBEs to `src` plus rungs 1..--take, through the same relay, and
           records every object: which track, block, object, HOW it arrived (subgroup
           stream or datagram), and WHEN (this host's monotonic clock).
  analyze  reconstructs blocks from a sub's saved arrivals at one or more deadlines.
           Runs on the subscriber host over its own saved file, so a second deadline
           needs no second run of the network.

## What "lost" means here

Source objects ride reliable QUIC subgroup streams; QUIC retransmits them, so every
one eventually arrives. An object therefore counts as lost if it has not arrived by
its DEADLINE: D seconds after the first arrival of ANY object (source or repair) of
the same block at the same subscriber. Both ends of that interval are read from the
subscriber's own clock, so nothing here assumes the hosts' clocks are synchronised.
Repair rides datagrams and is never retransmitted, so a lost repair datagram is gone.
There is no --drop here: loss comes from the network.

## The gate, and why it exists

A track's produce() starts when the relay first subscribes upstream for it, i.e. on
the FIRST downstream subscriber's demand; later subscribers share that one upstream
subscription. A second subscriber that reaches the relay half a second later joins
mid-stream and misses the early blocks,
so the two subscribers would not see the same blocks -- and the comparison between
them, which is the point, would be unfair. Every track's produce() therefore waits
until --gate-file exists. The orchestrator creates it once both subscribers report
SUBSCRIBED.

## Namespace

A public relay is shared, so each run uses its own namespace (--ns, one token, no
'/'). fecmoq.moq builds tracks under its module-level NAMESPACE; the publisher sets
that before constructing them. Nothing in src/fecmoq changes.
"""
import argparse
import asyncio
import inspect
import json
import logging
import os
import pickle
import sys
import time
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from aiomoqt.client import MOQTClient  # noqa: E402
from aiomoqt.track import SubscribedTrack, TrackState  # noqa: E402
from aiomoqt.types import (FilterType, MOQTMessageType, ObjectStatus, ParamType,  # noqa: E402
                           StreamResetCode, parse_draft_spec)
from aiomoqt.utils.logger import set_log_level  # noqa: E402
from aiomoqt.utils.url import parse_relay_url  # noqa: E402

import fecmoq.moq as fmoq  # noqa: E402
import fecmoq.symbols as fsym  # noqa: E402
from fecmoq.codec import Coder  # noqa: E402
from fecmoq.moq import SOURCE_TRACK, make_block, repair_track_name, rung_slices  # noqa: E402
from aiomoqt.delivery import StreamMapping  # noqa: E402

MIRROR_TRACK = "sym"     # the second (symbol) family when the publisher offers both
from fecmoq.symbols import SPAN, erased_symbols, from_symbols, layout_block, to_symbols  # noqa: E402
from fecmoq.wire import repair_codec, repair_index, unpack_repair  # noqa: E402


def say(*a):
    print(*a, flush=True)


def rungs_arg(s):
    return [float(x) for x in s.split(",")]


def client_for(a):
    r = parse_relay_url(a.relay)
    if not r.use_quic:
        # aiomoqt cannot send datagrams over WebTransport, so the repair tracks could not
        # be published at all: the publisher stays on raw QUIC. A subscriber may use WT
        # (https://...<endpoint path>): aiomoqt receives datagrams there.
        if a.role != "sub" or not a.relay.startswith("https://"):
            sys.exit(f"--relay must be moqt:// (raw QUIC; https:// for sub only), got {a.relay!r}")
        qlog = getattr(a, "qlog_dir", None)
        if qlog:
            os.makedirs(qlog, exist_ok=True)
        return MOQTClient(r.host, r.port, path=r.path or "", use_quic=False,
                          supported_drafts=a.draft, verify_tls=not a.insecure, qlog_dir=qlog or None)
    # --qlog-dir: picoquic writes a qlog of this connection (every frame, incl. the
    # MAX_STREAMS credit a subscriber grants). Diagnostic runs only: it costs CPU.
    qlog = getattr(a, "qlog_dir", None)
    if qlog:
        os.makedirs(qlog, exist_ok=True)
    # aiomoqt's parser keeps no path for moqt:// URLs; relays that route on it (moq-relay's
    # /anon) need it, so take it from the URL itself.
    from urllib.parse import urlparse
    path = r.path or urlparse(a.relay).path or ""
    ka = getattr(a, "keepalive", 0) or None     # QUIC PING interval; keeps an idle session (gate wait) up
    return MOQTClient(r.host, r.port, path=path, use_quic=True,
                      supported_drafts=a.draft, verify_tls=not a.insecure, qlog_dir=qlog or None,
                      keep_alive_interval=ka)


# --------------------------------------------------------------------- publisher

async def wait_for_file(path, poll=0.05):
    while not os.path.exists(path):
        await asyncio.sleep(poll)


class _Stamped:
    """Wraps produce()'s `out`: records when the track produced each object.
    `out` only queues into per-subscriber lanes; the lane hands the object to the
    session later (see stamp_deliveries)."""
    def __init__(self, out, name, sends):
        self._out, self._name, self._sends = out, name, sends

    async def write(self, group_id, object_id, payload, **kw):
        self._sends.setdefault((self._name, group_id, object_id), [time.monotonic(), None, None])
        await self._out.write(group_id, object_id, payload, **kw)

    def __getattr__(self, k):
        return getattr(self._out, k)


def stamp_deliveries(alias_name, sends):
    """Record, per object, when aiomoqt's per-subscriber lane handed it to the
    session (hand) and when the session write returned (done): stream_write_drain
    or dgram_write_drain, i.e. accepted by the transport. Wraps
    SubgroupDelivery.write in THIS process only; aiomoqt itself is unchanged."""
    from aiomoqt.delivery import SubgroupDelivery
    orig = SubgroupDelivery.write

    async def write(self, group_id, object_id, payload, **kw):
        rec = sends.setdefault((alias_name.get(self.track_alias, "?"), group_id, object_id),
                               [None, None, None])
        rec[1] = time.monotonic()
        sent = await orig(self, group_id, object_id, payload, **kw)
        rec[2] = time.monotonic()
        return sent
    SubgroupDelivery.write = write


def gated(cls, gate, sends):
    class Gated(cls):
        async def produce(self, out):
            await gate.wait()
            await super().produce(_Stamped(out, self.trackname, sends))
    Gated.__name__ = f"Gated{cls.__name__}"
    return Gated


class CatalogPublished(fmoq.PublishedTrack):
    """The MSF catalog as one Object (group 0, object 0) on a subgroup stream (--media)."""
    mapping = fmoq.StreamMapping.PER_GROUP

    def __init__(self, session, blob: bytes):
        super().__init__(session, fmoq.NAMESPACE, "catalog")
        self.blob = blob

    async def produce(self, out):
        await out.write(0, 0, self.blob, group_start=True)


class PrioSubscribedTrack(SubscribedTrack):
    """SubscribedTrack whose direct SUBSCRIBE carries a subscriber priority.
    aiomoqt 0.11.1's SubscribedTrack.subscribe() does not pass one through, though
    session.subscribe() takes it (d16: SUBSCRIBER_PRIORITY parameter). This is the
    direct-subscribe branch of SubscribedTrack.subscribe() with priority added."""
    def __init__(self, *args, priority=None, **kw):
        super().__init__(*args, **kw)
        self.sub_priority = priority

    async def subscribe(self, timeout: float = 30.0, **_):
        if self.sub_priority is None:
            return await super().subscribe(timeout=timeout)
        if self.on_object:
            self.session.on_object_received = self.on_object
        params = {}
        if self.auth_token is not None:
            params[ParamType.AUTH_TOKEN] = self.auth_token
        ok = await self.session.subscribe(
            namespace=self.namespace, track_name=self.trackname, forward=1,
            priority=self.sub_priority, filter_type=FilterType.LATEST_OBJECT,
            parameters=params, wait_response=True)
        if getattr(ok, "track_alias", None) is not None:
            self.track_alias = ok.track_alias
            if self.on_object:
                self.session.register_object_handler(self.track_alias, self.on_object)
        self._watch_done(getattr(ok, "request_id", None))
        self.state = TrackState.SUBSCRIBED


async def run_pub(a):
    """Announce the namespace, then serve each upstream SUBSCRIBE from the relay
    with ONE session-level handler that dispatches on track name -- the same shape
    as loopback_fec.py's server.

    Not PublishedTrack.publish() per track. aiomoqt's session keeps ONE handler per
    control-message type, and every publish() registers that track's own
    PUBLISH_OK / REQUEST_UPDATE handlers, so with several tracks on one session each
    publish() overwrites the last: every forward=1 reached the last track's handler
    and the others never produced (step 0, 2026-10-01: src/rep1/rep2 delivered
    nothing, rep3 everything). Here only one handler is ever registered."""
    fmoq.NAMESPACE = a.ns                     # see module docstring: per-run namespace
    gate = asyncio.Event()
    # Warm-up blocks are simply the first --warmup groups: same generator, same
    # pacing, sent before the measured window. The measured set -- groups
    # warmup .. warmup+blocks-1 -- is fixed by the command line before any data.
    total = a.warmup + a.blocks
    if a.pace_source and a.source_mapping != "datagram":
        sys.exit("--pace-source paces datagram source symbols: it needs --source-mapping datagram")
    if getattr(a, "media", None):
        # Shaka phase 2: real LOC frames, blocks of --objects frames,
        # a MoQ group per GOP of --gop-frames (default: one block per GOP)
        from fecmoq.media import MediaBlockSchedule, catalog_json
        schedule = MediaBlockSchedule(media=a.media, gop_frames=a.gop_frames or None, seed=a.seed,
                                      n_blocks=total, n_objects=a.objects, gap_s=a.gap,
                                      rung_fracs=a.rungs)
        catalog = catalog_json(schedule.media, n_objects=a.objects, gap_s=a.gap, rungs=a.rungs,
                               symbol_size=fsym.SYMBOL_SIZE, source=SOURCE_TRACK,
                               object_stride=fmoq.OBJECT_STRIDE)
        if schedule.blocks_per_group > 1 and (
                a.source_mapping != "datagram" or a.repair_mapping != "datagram"
                or a.mirror_mapping != "none" or a.repair_ids == "per-rung"
                or a.repair_group_per_symbol):
            sys.exit("--gop-frames: several blocks per group is built for datagram source and "
                     "repair only (no mirror, global repair ids)")
        say(f"MEDIA {a.media}: {len(schedule.media.blocks)} blocks of {a.objects}, "
            f"{schedule.blocks_per_group} per group, codec {schedule.media.codec}, "
            f"largest frame {schedule.media.largest} B; catalog {len(catalog)} B")
    else:
        schedule = fmoq.BlockSchedule(seed=a.seed, n_blocks=total, n_objects=a.objects,
                                      gap_s=a.gap, rung_fracs=a.rungs, gop=a.gop)
        catalog = None
    common = dict(seed=a.seed, n_blocks=total, n_objects=a.objects, gap_s=a.gap,
                  schedule=schedule)
    rung_of = {repair_track_name(r): r for r in range(1, len(a.rungs) + 1)}
    served = {}
    ending = []          # (track, session, relay request id) to end at exit
    sends = {}           # (track, group, object) -> [t_produce, t_hand, t_done], monotonic
    alias_name = {}
    stamp_deliveries(alias_name, sends)

    async def on_subscribe(session, msg):
        name = (msg.track_name or b"").decode()
        if name == "catalog" and catalog is not None:
            # served at once, not gated: a player configures itself from it before the media starts
            def make():
                return CatalogPublished(session, catalog)
            prio = a.prio_src
        elif name == SOURCE_TRACK:
            src_cls = {"group": fmoq.SourceTrack, "object": fmoq.ObjectSourceTrack,
                       "datagram": fmoq.DatagramSourceTrack,
                       "symbol-object": fmoq.SymbolStreamSourceTrack,
                       "symbol-subgroup": fmoq.SymbolSubgroupSourceTrack}[a.source_mapping]
            def make():
                t = gated(src_cls, gate, sends)(session, **common)
                t.spread = a.spread_source
                if a.source_mapping == "datagram":
                    t.pace = a.pace_source
                return t
            prio = a.prio_src
        elif name == MIRROR_TRACK and a.mirror_mapping != "none":
            # The other family, offered in the same run: the same blocks as symbols.
            mapping = {"datagram": StreamMapping.DATAGRAM,
                       "object": StreamMapping.PER_OBJECT}[a.mirror_mapping]
            def make():
                t = gated(fmoq.SymbolMirrorTrack, gate, sends)(session, mapping=mapping, **common)
                t.spread = a.spread_source
                return t
            prio = a.prio_src
        elif name in rung_of:
            rep_cls = {"datagram": fmoq.RepairTrack, "object": fmoq.ObjectRepairTrack,
                       "group": fmoq.GroupRepairTrack}[a.repair_mapping]
            def make():
                t = gated(rep_cls, gate, sends)(session, rung=rung_of[name], rung_fracs=a.rungs,
                                                **common)
                t.catch_up = a.repair_catch_up
                t.per_rung_ids = a.repair_ids == "per-rung"
                t.end_of_group = a.repair_eog
                t.group_per_symbol = a.repair_group_per_symbol
                t.pace = a.pace_repair
                return t
            prio = a.prio_rep
        else:
            say(f"UPSTREAM-SUBSCRIBE {name!r}: not ours, ignored")
            return
        dup = name in served
        track = served.get(name) or make()
        served[name] = track
        # Publisher priority (MoQT: lower = more urgent), carried in every subgroup
        # header and object datagram. aiomoqt 0.11.1 only signals it; it does not
        # map it to QUIC stream priority on this side.
        track.priority = prio
        ok = session.subscribe_ok(request_msg=msg)
        alias_name[ok.track_alias] = name
        sub_prio = getattr(msg, "priority", None)
        ft = getattr(msg, "filter_type", None)
        try:
            ft = FilterType(ft).name
        except (ValueError, TypeError):
            pass
        start = (f" start={msg.start_group}.{msg.start_object}"
                 if getattr(msg, "start_group", None) is not None else "")
        say(f"UPSTREAM-SUBSCRIBE wall={time.time():.3f} {name} alias={ok.track_alias} mapping={track.mapping.value} "
            f"publisher_priority={prio} "
            f"relay's subscriber_priority={sub_prio} relay's filter={ft}{start} "
            f"our content_exists={getattr(ok, 'content_exists', None)} "
            f"largest={getattr(ok, 'largest_group_id', None)}.{getattr(ok, 'largest_object_id', None)}"
            f"{' (again)' if dup else ''}")
        track.track_alias = ok.track_alias
        track._generating = True
        # The relay's request id, kept to end this subscription at exit. Deliberately NOT
        # put on the subscription now: with it there, aiomoqt sends PUBLISH_DONE the
        # moment production ends, and aiomoqt 0.11.1's subscriber answers PUBLISH_DONE by
        # STOP_SENDING every stream of the track still open -- it does not wait for the
        # announced stream count -- which cut the last group mid-retransmission
        # (milestone 4, first grid). So PUBLISH_DONE goes out after the linger instead.
        ending.append((track, session, msg.request_id))
        # For a joining FETCH naming this subscription: its Joining Location is the Largest
        # Location now (aiomoqt's PublishedTrack records the same in its own _on_subscribe).
        fs = fetcher(session, name)
        joins[msg.request_id] = (name, fs._largest if fs else None)
        # Log (only) when the relay cancels this upstream subscription: d16 UNSUBSCRIBE, or
        # a terminated request stream at d18. Answers "does the relay drop its upstream
        # subscription to a rung once no subscriber wants it?"
        t_sub = time.monotonic()
        session.register_request_cancel_handler(
            msg.request_id,
            lambda rid, name=name, t_sub=t_sub: say(
                f"UPSTREAM-UNSUBSCRIBE wall={time.time():.3f} {name} request_id={rid} "
                f"{time.monotonic() - t_sub:.1f}s after its SUBSCRIBE"))
        await track.generate(session, ok.track_alias)

    fetch_stats = defaultdict(int)
    # FETCH (on by default; --no-serve-fetch for the behaviour before 2026-10-24, aiomoqt's refusal):
    # answered from the schedule (fmoq.ScheduleHistory) through aiomoqt's own PublishedTrack FETCH
    # code, one fmoq.FetchServer per track name -- whether or not anyone subscribed to it, since the
    # relay sends upstream the FETCH of a rung it never saw. --fetch-window seconds of blocks are held.
    window_blocks = max(1, round(a.fetch_window / a.gap))
    fetchers = {}
    joins = {}           # relay's SUBSCRIBE request id -> (track, Largest Location when it arrived)

    def fetch_spec(name):
        if name == SOURCE_TRACK:
            return fmoq.FetchSpec(kind="source", source_mapping=a.source_mapping), a.prio_src
        if name == MIRROR_TRACK and a.mirror_mapping != "none":
            return fmoq.FetchSpec(kind="mirror", mapping={"datagram": StreamMapping.DATAGRAM,
                                                          "object": StreamMapping.PER_OBJECT}[a.mirror_mapping]), a.prio_src
        if name in rung_of:
            mapping = {"datagram": StreamMapping.DATAGRAM, "object": StreamMapping.PER_OBJECT,
                       "group": StreamMapping.PER_GROUP}[a.repair_mapping]
            return fmoq.FetchSpec(kind="repair", mapping=mapping, rung=rung_of[name],
                                  per_rung_ids=a.repair_ids == "per-rung",
                                  group_per_symbol=a.repair_group_per_symbol,
                                  end_of_group=a.repair_eog), a.prio_rep
        return None, None

    def fetcher(session, name):
        if name not in fetchers:
            spec, prio = fetch_spec(name)
            fetchers[name] = spec and fmoq.FetchServer(
                session, name, fmoq.ScheduleHistory(schedule, spec, window_blocks, priority=prio))
        return fetchers[name]

    async def on_fetch(session, msg):
        """Repair by FETCH (design note 5.5), standalone or joining, for the source track, the
        mirror and every rung. Whatever is not ours (the catalog, an unknown name, a joining FETCH
        for no subscription of ours) gets aiomoqt's own refusal."""
        from aiomoqt.types import FetchType
        try:
            ftype = FetchType(msg.fetch_type).name.lower()
        except ValueError:
            ftype = str(msg.fetch_type)
        joining = msg.fetch_type in (FetchType.RELATIVE_JOINING, FetchType.ABSOLUTE_JOINING)
        if joining:
            name, anchor = joins.get(msg.joining_request_id, (None, None))
        else:
            name = (msg.track_name or b"").decode()
        fs = fetcher(session, name) if name else None
        if fs is None:
            fetch_stats["refused"] += 1
            say(f"FETCH-REFUSED wall={time.time():.3f} {name} type={ftype}")
            await session._handle_fetch(msg)
            return
        if joining:
            fs._sub_for(session).joining[msg.joining_request_id] = anchor
        fs._done = schedule.last_out >= schedule.n_blocks - 1
        fs._history.last = None
        t = time.monotonic()
        try:
            await fs._on_fetch(session, msg)
        except Exception as exc:                # noqa: BLE001  the relay may cancel it
            fetch_stats["failed"] += 1
            say(f"FETCH-FAILED wall={time.time():.3f} {name} type={ftype}: {exc!r}")
            return
        last = fs._history.last
        if last is None:                         # refused by aiomoqt's range checks
            fetch_stats["refused"] += 1
            say(f"FETCH-REFUSED wall={time.time():.3f} {name} type={ftype} "
                f"start={getattr(msg, 'start_group', None)}.{getattr(msg, 'start_object', None)} "
                f"largest={fs._largest}")
            return
        n, beyond, start, end = last
        fetch_stats["served"] += 1
        fetch_stats["objects"] += n
        fetch_stats["beyond_window"] += beyond
        say(f"FETCH-SERVED wall={time.time():.3f} {name} type={ftype} "
            f"{start[0]}.{start[1]}-{end[0]}.{min(end[1], 99999)} objects={n}"
            f"{' +end-of-unknown-range' if beyond else ''} in {1e3 * (time.monotonic() - t):.1f} ms")

    async with client_for(a).connect() as session:
        await session.client_session_init()
        session.register_handler(MOQTMessageType.SUBSCRIBE, on_subscribe)
        if a.serve_fetch:
            session.register_handler(MOQTMessageType.FETCH, on_fetch)
            sizes = [len(o) for o in schedule.block(0)]
            say(f"FETCH serving on: window {a.fetch_window:g} s = {window_blocks} blocks of every track "
                f"(source, mirror, rungs; recomputed from the schedule, no byte bound; ~{window_blocks * sum(sizes) / 1e6:.1f} MB "
                f"of source at block 0's size); no END_OF_GROUP status objects in the answers")
        else:
            say("FETCH serving off (--no-serve-fetch): aiomoqt refuses every FETCH")
        reply = await session.publish_namespace(namespace=a.ns, wait_response=True)
        kind = type(reply).__name__
        if "Error" in kind:
            sys.exit(f"PUBLISH_NAMESPACE refused by the relay: {reply}")
        say(f"PUBLISHED-NAMESPACE {a.ns}: {kind}")
        say(f"PUBLISHED-ALL namespace announced; serving src + rep1..rep{len(a.rungs)} "
            f"on demand; waiting for gate {a.gate_file}")

        await wait_for_file(a.gate_file)
        gate.set()
        t0 = time.monotonic()
        # picoquic's cumulative wire counters for the ONE connection this publisher has,
        # to the relay: bytes placed on the wire, retransmissions and framing included.
        # (path_quality()'s byte fields are per-call deltas, not totals.)
        # Each read returns the PREVIOUS worker snapshot and asks for a fresh one, so
        # read, let the worker answer, read again.
        async def wire():
            qc = getattr(session, "_quic", None)
            if qc is None:
                return (0, 0)
            qc.bytes_sent
            await asyncio.sleep(0.05)
            return (qc.bytes_sent, qc.bytes_received)
        up0 = await wire()
        say(f"CODER {schedule.coder} {schedule.encode_mode} repair-after={schedule.repair_after}")
        say(f"GATE-OPEN producing {a.warmup} warm-up + {a.blocks} measured blocks "
            f"x {a.objects} objects, gap {a.gap}s")
        await asyncio.sleep(total * a.gap + a.linger)
        up1 = await wire()
        up = {"bytes_sent": up1[0] - up0[0], "bytes_received": up1[1] - up0[1]}
        say(f"UPSTREAM-STREAM-DATA picoquic stream data only (no datagrams, no framing), gate "
            f"to end of linger: sent={up['bytes_sent']} received={up['bytes_received']} -- "
            f"publisher-to-relay wire bytes are in relay-bytes.txt (netfilter)")
        if a.abrupt_exit:
            say("EXIT abrupt: leaving without ending tracks or withdrawing the namespace")
        else:
            # Production is over and the linger let every stream and retransmission
            # finish. Now end each track (PUBLISH_DONE, TRACK_ENDED, with its stream
            # count), withdraw the namespace, give it a moment to leave, then close.
            for track, sess, rid in ending:
                sub = track._sub_for(sess)
                sub.subscribe_request_id = rid
                sub.subscribers.add(rid)
                track._send_publish_done(sess)
            session.publish_namespace_done(namespace=session._make_namespace_tuple(a.ns),
                                           request_id=getattr(reply, "request_id", None))
            await asyncio.sleep(1.0)
            say(f"EXIT clean: PUBLISH_DONE on {len(ending)} subscriptions after the linger, "
                "then PUBLISH_NAMESPACE_DONE, then close")
        # Objects the publisher itself dropped because the relay session fell behind
        # (aiomoqt sheds rather than queue without bound). Nonzero would mean some
        # "network loss" downstream was really sender-side shedding -- worth knowing
        # before attributing loss to the path.
        for name in [SOURCE_TRACK] + ([MIRROR_TRACK] if a.mirror_mapping != "none" else []) + list(rung_of):
            t = served.get(name)
            if t is None:
                say(f"  {name}: never subscribed upstream")
            else:
                cu = getattr(t, "caught_up", 0)
                say(f"  {name}: served  shed={sum(t.shed.values()) if t.shed else 0}"
                    f"{f'  catch-up replays={cu}' if cu else ''}")
        if a.serve_fetch:
            say(f"  FETCH: {dict(fetch_stats) or 'none received'}")
        os.makedirs(a.out, exist_ok=True)
        if a.repair_group_per_symbol:
            # record repair as (rung, block, global index), as every other mapping does
            sends = {((k[0], k[1] // fmoq.GROUP_STRIDE, k[1] % fmoq.GROUP_STRIDE)
                      if k[0] != SOURCE_TRACK else k): v for k, v in sends.items()}
        with open(os.path.join(a.out, "pub.sends.pkl"), "wb") as f:
            pickle.dump(dict(warmup=a.warmup, blocks=a.blocks, gate_mono=t0,
                             encode_s=schedule.encode_s, encode_total_s=schedule.encode_total_s,
                             coder=schedule.coder, encode_mode=schedule.encode_mode,
                             repair_after=schedule.repair_after, sent_wait_s=schedule.sent_wait_s,
                             sent_timeouts=schedule.sent_timeouts,
                             gap=a.gap, source_mapping=a.source_mapping,
                             repair_mapping=a.repair_mapping, mirror_mapping=a.mirror_mapping,
                             repair_catch_up=a.repair_catch_up, repair_ids=a.repair_ids,
                             repair_eog=a.repair_eog, spread_source=a.spread_source, upstream=up,
                             symbol_size=fsym.SYMBOL_SIZE, pace_repair=a.pace_repair,
                             prio_src=a.prio_src, prio_rep=a.prio_rep,
                             sends={k: tuple(v) for k, v in sends.items()}), f)
        say(f"  send times recorded for {len(sends)} objects -> {a.out}/pub.sends.pkl")
        enc = sorted(schedule.encode_s.values())
        tot = sorted(schedule.encode_total_s.values())
        if enc:
            say(f"  coder {schedule.coder} ({schedule.encode_mode}): encode on the critical path "
                f"p50 {1e3*enc[len(enc)//2]:.3f} ms max {1e3*enc[-1]:.3f} ms; all encode work "
                f"p50 {1e3*tot[len(tot)//2]:.3f} ms, over {len(enc)} blocks")
        sw = sorted(schedule.sent_wait_s.values())
        if sw:
            say(f"  repair-after=sent: wait for the source to leave the send buffer p50 "
                f"{1e3*sw[len(sw)//2]:.2f} ms p95 {1e3*sw[int(.95*len(sw))]:.2f} ms max "
                f"{1e3*sw[-1]:.2f} ms; timeouts {schedule.sent_timeouts}/{len(sw)}")
        say(f"PUB-DONE after {time.monotonic() - t0:.1f}s")


# -------------------------------------------------------------------- subscriber

def arrival_mode(msg, subgroup_id):
    """How an object reached us. Classified by message type, cross-checked against
    the subgroup id aiomoqt passes (None for datagrams). They should agree; a
    stream whose subgroup id could not be resolved would make subgroup_id alone
    misreport it as a datagram, so the type is the primary signal."""
    by_type = "datagram" if "Datagram" in type(msg).__name__ else "stream"
    by_sgid = "datagram" if subgroup_id is None else "stream"
    return by_type, by_type == by_sgid


async def run_sub(a):
    recs = []          # (track, group, object, mode, t_mono, payload)
    status_objs = defaultdict(int)
    mismatches = []
    first_t = None
    subgroups = defaultdict(set)   # (track, group) -> subgroup ids seen in object callbacks
    streams = []       # (quic stream id, track alias, group, subgroup or None, t_mono): one per
                       # incoming subgroup stream, from aiomoqt's admission hook
    # --stop-decoded: once a block holds k distinct symbols (source + repair), the rest
    # of its streams carry nothing it needs, so ask the relay to stop sending them.
    # Only meaningful for the symbol families on streams (arm D).
    got = set()                    # (track, group, object) received
    k_of, have, decoded_at = {}, defaultdict(set), {}
    stopped = []                   # (stream id, track, group, subgroup, t_mono)
    alias_name = {}
    holder = {}

    def stop_rest(group_id, t):
        decoded_at[group_id] = t
        sess = holder.get("session")
        for sid, alias, g, sg, _ts in streams:
            name = alias_name.get(alias)
            if g != group_id or sg is None or name is None or (name, g, sg) in got:
                continue
            try:
                sess.stream_stop_sending(sid, StreamResetCode.CANCELLED)
                stopped.append((sid, name, g, sg, t))
            except Exception as exc:            # noqa: BLE001  a stream already gone
                stopped.append((sid, name, g, sg, -1.0))
                say(f"STOP_SENDING stream {sid} failed: {exc!r}")

    def make_cb(track):
        def cb(msg, consumed, now, group_id, subgroup_id=None):
            nonlocal first_t
            t = time.monotonic()
            st = getattr(msg, "status", ObjectStatus.NORMAL)
            if st != ObjectStatus.NORMAL:
                # END_OF_GROUP and friends come through the same callback. Never data
                # (milestone 2b's first-run bug). Counted, not stored.
                status_objs[(track, getattr(st, "name", str(st)))] += 1
                return
            mode, agree = arrival_mode(msg, subgroup_id)
            if subgroup_id is not None:
                subgroups[(track, group_id)].add(subgroup_id)
            if not agree:
                mismatches.append((track, group_id, msg.object_id, type(msg).__name__, subgroup_id))
            if first_t is None:
                first_t = t
            oid = msg.object_id
            if track != SOURCE_TRACK:
                # --repair-ids per-rung: the object id is the position in the rung; the
                # global repair index the decoder needs is in the payload. Recorded as the
                # global index, so the analysis is the same either way.
                try:
                    idx = repair_index(bytes(msg.payload))
                except Exception:              # noqa: BLE001  not a repair payload
                    idx = None
                if idx is not None:
                    oid = idx
                if a.repair_group_per_symbol:
                    group_id //= fmoq.GROUP_STRIDE   # group = block * stride + index
            recs.append((track, group_id, oid, mode, t, bytes(msg.payload)))
            if a.adaptive and track == SOURCE_TRACK:
                src_seen[group_id].add(msg.object_id)
                src_first.setdefault(group_id, t)
                if msg.object_id >= src_hi.get(group_id, (-1, 0))[0]:
                    src_hi[group_id] = (msg.object_id, t)
                if group_id not in src_k:
                    try:
                        src_k[group_id] = layout_block(unpack_repair(bytes(msg.payload))[0], SPAN).k
                    except Exception:          # noqa: BLE001
                        src_k[group_id] = None
            if a.stop_decoded and group_id not in decoded_at:
                got.add((track, group_id, msg.object_id))
                if group_id not in k_of:
                    try:
                        k_of[group_id] = layout_block(unpack_repair(bytes(msg.payload))[0], SPAN).k
                    except Exception:          # noqa: BLE001  not a symbol payload
                        k_of[group_id] = None
                if k_of[group_id]:
                    have[group_id].add((track == SOURCE_TRACK, oid))
                    if len(have[group_id]) >= k_of[group_id]:
                        stop_rest(group_id, t)
        return cb

    # The source is subscribed under a.source_track (src, or the mirror family's
    # sym) and recorded as SOURCE_TRACK either way, so the analysis is unchanged.
    names = [repair_track_name(r) for r in range(1, a.take + 1)] + [a.source_track]
    if a.adaptive:
        names = [a.source_track]          # rungs only when loss is seen (adapt() below)
    adapt_events = []                     # (kind, t_mono, t_wall, detail)
    adapt_judged = []                     # (block, t_mono, lossy, seen, k) per judgement
    sub_requests = []                     # every SUBSCRIBE sent (filter, SUBSCRIBE_OK)
    src_seen, src_k, src_first, src_hi = defaultdict(set), {}, {}, {}
    tracks = {}
    done_t = {}        # track -> when its PUBLISH_DONE arrived
    async with client_for(a).connect() as session:
        await session.client_session_init()
        # Record every incoming subgroup stream (stream id, alias, group, subgroup). Wraps
        # the session's own admission method on this instance only; aiomoqt is unchanged.
        admit = session._admit_subgroup_stream

        def admit_and_note(stream_id, header):
            streams.append((stream_id, header.track_alias, header.group_id,
                            getattr(header, "subgroup_id", None), time.monotonic()))
            return admit(stream_id, header)
        session._admit_subgroup_stream = admit_and_note
        holder["session"] = session
        # The relay's QUIC transport parameters on this leg (max_datagram_frame_size among
        # them), for raw QUIC and WT alike.
        try:
            peer_tp = session.peer_transport_parameters
        except Exception as e:                       # noqa: BLE001
            peer_tp = repr(e)
        say(f"TRANSPORT {'wt' if getattr(session, '_is_wt', False) else 'quic'} "
            f"peer max_datagram_frame_size="
            f"{(peer_tp or {}).get('max_datagram_frame_size') if isinstance(peer_tp, dict) else peer_tp}")
        # Every SUBSCRIBE this subscriber sends: the filter it asked for and what the
        # relay's SUBSCRIBE_OK said (live adaptation: is a re-subscription served from
        # the relay's live upstream subscription or does it reach the publisher?).
        orig_subscribe = session.subscribe
        try:
            default_filter = inspect.signature(orig_subscribe).parameters["filter_type"].default
        except (KeyError, ValueError, TypeError):
            default_filter = None

        async def subscribe_and_note(*args, **kw):
            t, w = time.monotonic(), time.time()
            ok = await orig_subscribe(*args, **kw)
            ft = kw.get("filter_type", default_filter)
            name = kw.get("track_name")
            sub_requests.append(dict(
                track=name.decode() if isinstance(name, bytes) else name, t_mono=t, t_wall=w,
                t_ok=time.monotonic(), filter=getattr(ft, "name", ft),
                start=(kw.get("start_group"), kw.get("start_object")),
                content_exists=getattr(ok, "content_exists", None),
                largest=(getattr(ok, "largest_group_id", None), getattr(ok, "largest_object_id", None)),
                alias=getattr(ok, "track_alias", None)))
            say(f"SUBSCRIBE-SENT {sub_requests[-1]['track']} filter={sub_requests[-1]['filter']} "
                f"ok after {1e3 * (sub_requests[-1]['t_ok'] - t):.0f} ms "
                f"content_exists={sub_requests[-1]['content_exists']} largest={sub_requests[-1]['largest']}")
            return ok
        session.subscribe = subscribe_and_note
        # Repair rungs first, so no early repair datagram beats its handler.
        for n in names:
            is_src = n == a.source_track
            t = PrioSubscribedTrack(session, namespace=a.ns, trackname=n,
                                    on_object=make_cb(SOURCE_TRACK if is_src else n),
                                    priority=a.prio_src if is_src else a.prio_rep)
            t.on_done = lambda msg, n=n: done_t.setdefault(n, time.monotonic())
            await t.subscribe(timeout=a.subscribe_timeout)
            tracks[n] = t
            alias_name[t.track_alias] = SOURCE_TRACK if is_src else n
        say(f"SUBSCRIBED {a.label} ns={a.ns} tracks={','.join(names)}")

        async def adapt():
            # Live adaptation (arm B). Subscribe rungs 1..take as soon as the newest judged
            # block is lossy; unsubscribe once QUIET judged blocks in a row since then are
            # clean. How a block is judged (--adapt-detect):
            #   due   GRACE s after its last source symbol was due: first arrival + the
            #         spread of its emission (spread * gap * (n-1)/n; 0 for burst, which is
            #         the rule before 2026-10-05) -- lossy if any source symbol is missing.
            #   hole  lossy once a symbol below the highest index received has been missing
            #         GRACE s, or (a missing tail) GRACE s after the next block's first
            #         symbol; clean once complete. Independent of the emission timing.
            GRACE, QUIET = 0.15, 10
            due = GRACE + a.spread_source * a.gap * (a.objects - 1) / a.objects
            judged, on, subs, since = {}, False, [], None
            while True:
                now = time.monotonic()
                for g, t1 in list(src_first.items()):
                    k = src_k.get(g)
                    if g in judged or not k:
                        continue
                    if a.adapt_detect == "due":
                        if now - t1 >= due:
                            judged[g] = len(src_seen[g]) < k
                    else:
                        hi, t_hi = src_hi[g]
                        if len(src_seen[g]) >= k:
                            judged[g] = False
                        elif len(src_seen[g]) < hi + 1 and now - t_hi >= GRACE:
                            judged[g] = True
                        elif g + 1 in src_first and now - src_first[g + 1] >= GRACE:
                            judged[g] = True
                    if g in judged:
                        # (block, when, lossy, symbols seen, k): a lossy judgement whose missing
                        # symbols all arrive later was a false trigger (reordering or delay)
                        adapt_judged.append((g, now, judged[g], len(src_seen[g]), k))
                order = sorted(judged)
                if order and not on and judged[order[-1]]:
                    on, since = True, order[-1]
                    adapt_events.append(("subscribe", time.monotonic(), time.time(), order[-1]))
                    say(f"ADAPT subscribe rungs 1..{a.take} after lossy block {order[-1]}")
                    subs = []
                    for r in range(1, a.take + 1):
                        n = repair_track_name(r)
                        t = PrioSubscribedTrack(session, namespace=a.ns, trackname=n,
                                                on_object=make_cb(n), priority=a.prio_rep)
                        await t.subscribe(timeout=a.subscribe_timeout)
                        alias_name[t.track_alias] = n
                        tracks[f"{n}#{len(adapt_events)}"] = t
                        subs.append(t)
                    adapt_events.append(("subscribed", time.monotonic(), time.time(), order[-1]))
                elif on:
                    after = [g for g in order if g > since]
                    if len(after) >= QUIET and not any(judged[g] for g in after[-QUIET:]):
                        on = False
                        adapt_events.append(("unsubscribe", time.monotonic(), time.time(), order[-1]))
                        say(f"ADAPT unsubscribe rungs after {QUIET} clean blocks (to {order[-1]})")
                        for t in subs:
                            session.unsubscribe(t.request_id)
                    elif any(judged[g] for g in after):
                        since = max(g for g in after if judged[g])
                await asyncio.sleep(0.05)
        adapt_task = asyncio.ensure_future(adapt()) if a.adaptive else None
        # When (if ever) the session closes under us, against the data: a relay that drops
        # a subscriber mid-run looks different from an idle close after the data ended.
        closed_at = None
        # picoquic's own per-connection counters, sampled through the run: wire bytes
        # received (source + repair + whatever the relay retransmitted + framing). It has
        # no retransmitted-byte counter, and the retransmitting sender is the relay, so
        # the retransmission share is NOT derived from this.
        pq = []
        t_end = time.monotonic() + a.duration
        while time.monotonic() < t_end:
            if closed_at is None and session._close_err is not None:
                closed_at = time.monotonic()
            q = session._quic.path_quality() if getattr(session, "_quic", None) else {}
            if q:
                pq.append((time.monotonic(), q["bytes_received"], q["bytes_sent"], q["sent"],
                           q["lost"], q["rtt"], q["cwin"]))
            await asyncio.sleep(0.25)
        if adapt_task is not None:
            adapt_task.cancel()
        # Did the relay end any subscription or the session before we left?
        last_data = max((r[4] for r in recs), default=None)
        ended = {n: repr(getattr(t, "publish_done", None))
                 + (f" at {done_t[n] - last_data:+.1f} s after the last data object"
                    if n in done_t and last_data is not None else "")
                 for n, t in tracks.items() if getattr(t, "publish_done", None) is not None}
        close_err = session._close_err

    meta = dict(label=a.label, ns=a.ns, take=a.take, rungs=a.rungs, seed=a.seed,
                warmup=a.warmup, blocks=a.blocks, objects=a.objects, gop=a.gop, relay=a.relay, draft=a.draft,
                sub_prio_src=a.prio_src, sub_prio_rep=a.prio_rep, source_track=a.source_track,
                alias_of={n: t.track_alias for n, t in tracks.items()},
                subgroups={f"{k[0]}:{k[1]}": sorted(v) for k, v in subgroups.items()},
                streams=streams, publish_done=ended, session_close_err=repr(close_err),
                transport=pq, sub_transport="wt" if getattr(session, "_is_wt", False) else "quic",
                peer_transport_parameters=peer_tp,
                session_closed_after_last_data_s=(closed_at - max(r[4] for r in recs))
                if closed_at is not None and recs else None,
                status_objects={f"{k[0]}:{k[1]}": v for k, v in status_objs.items()},
                stop_decoded=a.stop_decoded, stopped=stopped, decoded_at=decoded_at,
                adaptive=a.adaptive, adapt_detect=a.adapt_detect, adapt_events=adapt_events,
                adapt_judged=adapt_judged,
                sub_requests=sub_requests,
                mode_mismatches=mismatches)
    os.makedirs(a.out, exist_ok=True)
    raw = os.path.join(a.out, f"{a.label}.arrivals.pkl")
    with open(raw, "wb") as f:
        pickle.dump(dict(meta=meta, recs=recs), f)
    with open(os.path.join(a.out, f"{a.label}.arrivals.csv"), "w") as f:
        f.write("track,group,object,mode,t_rel_s,bytes\n")
        for tr, g, o, m, t, p in recs:
            f.write(f"{tr},{g},{o},{m},{t - (first_t or t):.6f},{len(p)}\n")
    print_modes(meta, recs)
    print_streams(meta)
    if a.deadlines:
        report(meta, recs, a.deadlines, os.path.join(a.out, f"{a.label}.result.json"))
    # Last line, deliberately: the orchestrator collects results as soon as it sees
    # this, so everything above must already be on disk.
    say(f"SUB-DONE {a.label} objects={len(recs)} raw={raw}")


# ---------------------------------------------------------------------- analysis

def measured(meta):
    """The measured blocks: group ids warmup .. warmup+blocks-1. Fixed by the
    command line before the run; warm-up groups are never analysed."""
    w = meta.get("warmup", 0)
    return range(w, w + meta["blocks"])


def print_modes(meta, recs):
    by = defaultdict(lambda: [0, 0])
    win = measured(meta)
    warm = 0
    for tr, g, _o, m, _t, _p in recs:
        if g not in win:
            warm += 1
            continue
        by[tr][0 if m == "stream" else 1] += 1
    order = [SOURCE_TRACK] + [repair_track_name(r) for r in range(1, len(meta["rungs"]) + 1)]
    say(f"\n  arrival mode, {meta['label']} (take={meta['take']}), measured blocks "
        f"{win.start}..{win.stop - 1}  ({warm} warm-up objects not counted):")
    say(f"  {'track':<6} {'stream':>7} {'datagram':>9}")
    for tr in order:
        s, d = by.get(tr, [0, 0])
        say(f"  {tr:<6} {s:>7} {d:>9}")
    if meta["status_objects"]:
        say(f"  status objects (not data): {meta['status_objects']}")
    if meta["mode_mismatches"]:
        say(f"  ⚠️ {len(meta['mode_mismatches'])} objects where message type and subgroup id "
            f"disagree on arrival mode: {meta['mode_mismatches'][:5]}")


def print_streams(meta):
    """Gate checks for one-stream-per-object source (milestone 4 arm C): QUIC streams and
    subgroups per group, per track, and whether the relay ended anything early."""
    if "streams" not in meta:
        return
    name_of = {v: k for k, v in meta["alias_of"].items()}
    per = defaultdict(lambda: defaultdict(int))
    for _sid, alias, g, _sg, _t in meta["streams"]:
        per[name_of.get(alias, alias)][g] += 1
    for tr in sorted(per):
        counts = sorted(per[tr].values())
        sg = [len(v) for k, v in meta["subgroups"].items() if k.split(":")[0] == tr]
        say(f"  streams {tr}: {sum(counts)} QUIC streams over {len(counts)} groups, per group "
            f"min {counts[0]} max {counts[-1]}; distinct subgroups per group min "
            f"{min(sg) if sg else '-'} max {max(sg) if sg else '-'}")
    ids = sorted(s[0] for s in meta["streams"])
    if ids:
        say(f"  stream ids: lowest {ids[0]}, highest {ids[-1]} -> {ids[-1] // 4 + 1} "
            f"server-initiated uni streams accepted (credit consumed)")
    gap = meta.get("session_closed_after_last_data_s")
    say(f"  relay ended: publish_done={meta['publish_done'] or 'none'}; "
        f"session close={meta['session_close_err']}"
        + (f", {gap:+.1f} s after the last data object" if gap is not None else ""))


def block_view(recs):
    """block -> {'src': {obj: (t, payload)}, 'rep': {global_idx: (t, payload)}}."""
    blocks = defaultdict(lambda: {"src": {}, "rep": {}})
    for tr, g, o, _m, t, p in recs:
        side = "src" if tr == SOURCE_TRACK else "rep"
        prev = blocks[g][side].get(o)
        if prev is None or t < prev[0]:      # a duplicate keeps its earliest arrival
            blocks[g][side][o] = (t, p)
    return blocks


def annotate(meta, recs):
    """Mark a run whose source arrived as symbols (milestone 4 arm B: one source
    symbol per datagram, object id = symbol index). Inferred from the arrivals, so it
    also covers runs recorded before the flag existed. Returns meta."""
    if "src_symbols" not in meta:
        # Symbols arrive as datagrams (arm B) or, in arm D, on streams with object ids
        # running to k (~120) where source objects stop at the object count.
        # A zero-length object is a status (e.g. END_OF_GROUP, which aiomoqt surfaces as an
        # object on draft 14), not a symbol: it must not flip stream source to "symbols".
        meta["src_symbols"] = any(tr == SOURCE_TRACK and len(p) > 0 and (m == "datagram" or o >= meta["objects"])
                                  for tr, _g, o, m, _t, p in recs)
    return meta


def _reconstruct_symbols(meta, b, view, t0, D, allowed):
    """Arm B: block b from source SYMBOLS plus repair, both datagrams. A source symbol
    arriving after D counts as erased, like a lost one; nothing is retransmitted."""
    truth = make_block(meta["seed"], b, meta["objects"], meta.get("gop", 0))
    src = {j: p for j, (t, p) in view["src"].items() if t - t0 <= D}
    reps = {j: p for j, (t, p) in view["rep"].items() if t - t0 <= D and j in allowed}
    late = sum(1 for t, _ in view["src"].values() if t - t0 > D)
    anyp = next(iter(view["src"].values()), None) or next(iter(view["rep"].values()), None)
    # Decode with the coder that produced this run's objects (first payload byte), so the
    # analysis needs no FECMOQ_CODER of its own.
    codec = repair_codec(anyp[1]) if anyp else None
    sizes = unpack_repair(anyp[1], expect=codec)[0] if anyp else [len(o) for o in truth]
    lay = layout_block(sizes, SPAN)
    avail = {j: unpack_repair(p, expect=codec)[1] for j, p in src.items() if j < lay.k}
    decode_s, decode_bad = 0.0, False       # decode time on THIS host (post-run analysis)
    if len(avail) == lay.k:                  # every source symbol in time: no decode needed
        good = from_symbols(lay, [avail[j] for j in range(lay.k)]) == truth
    else:
        avail.update({lay.k + j: unpack_repair(p, expect=codec)[1] for j, p in reps.items()})
        out = None
        if len(avail) >= lay.k:
            t_dec = time.perf_counter()
            out = Coder(lay.k, 0, codec).decode(avail)
            decode_s = time.perf_counter() - t_dec
        good = out is not None and from_symbols(lay, out) == truth
        decode_bad = len(avail) >= lay.k and not good
    missing = lay.k - len(src)
    return good, dict(block=b, ok=good, lost=missing, late=late, decode_s=decode_s,
                      decode_bad=decode_bad, codec=codec,
                      never=lay.k - len(view["src"]), erased=missing, k=lay.k,
                      rep_in_time=len(reps), rep_total=len(view["rep"]))


def block_timings(meta, blocks, rungs_used=None, t0_of=None):
    """Per measured block, the two moments that decide recovery, as offsets from the
    block's clock start (t0), independent of D:

      src_done  when the block's whole source had arrived (inf if it never did): from
                then on no decode is needed.
      ready     the first arrival after which the symbols in hand reach k while some
                source is still missing (inf if never): from then on it can be decoded.
      decode_s  the time THIS host takes to decode the block from exactly the symbols in
                hand at `ready` (measured now: this runs on the subscriber VM).
      decode_ok that decode returned the seed's bytes.

    Recovered by D, decode time excluded (the original metric): src_done <= D, or ready
    <= D and decode_ok. Decode time included: src_done <= D, or ready + decode_s <= D and
    decode_ok. rungs_used as in reconstruct_at."""
    take = meta["take"] if rungs_used is None else rungs_used
    out = {}
    symbols_src = meta.get("src_symbols")
    for b in measured(meta):
        truth = make_block(meta["seed"], b, meta["objects"], meta.get("gop", 0))
        view = blocks.get(b, {"src": {}, "rep": {}})
        ts = [t for t, _ in view["src"].values()] + [t for t, _ in view["rep"].values()]
        if not ts:
            out[b] = dict(src_done=float("inf"), ready=float("inf"), decode_s=0.0, decode_ok=False)
            continue
        t0 = t0_of.get(b, min(ts)) if t0_of is not None else min(ts)
        # The coder byte is on repair objects, and on source objects only when source is
        # symbols (arm B); stream-source objects are plain payload.
        anyp = next(iter(view["src"].values()), None) or next(iter(view["rep"].values()), None)
        if view["rep"]:
            codec = repair_codec(next(iter(view["rep"].values()))[1])
        elif symbols_src:
            codec = repair_codec(anyp[1])
        else:
            codec = None
        if symbols_src:
            sizes = unpack_repair(anyp[1], expect=codec)[0]
        else:
            sizes = (unpack_repair(next(iter(view["rep"].values()))[1], expect=codec)[0]
                     if view["rep"] else [len(o) for o in truth])
        lay = layout_block(sizes, SPAN)
        allowed = set()
        for sl in rung_slices(lay.k, meta["rungs"])[:take]:
            allowed.update(sl)
        n_src = lay.k if symbols_src else len(truth)
        ev = sorted([(t - t0, "s", i) for i, (t, _p) in view["src"].items() if i < n_src] +
                    [(t - t0, "r", j) for j, (t, _p) in view["rep"].items() if j in allowed])
        have_s, have_r = set(), set()
        src_done = ready = float("inf")
        snap = None
        for off, kind, idx in ev:
            (have_s if kind == "s" else have_r).add(idx)
            missing = set(range(n_src)) - have_s
            if not missing:
                src_done = off
                break
            n_avail = (lay.k - len(missing)) if symbols_src else \
                (lay.k - len(erased_symbols(lay, missing)))
            if ready == float("inf") and n_avail + len(have_r) >= lay.k:
                ready, snap = off, (set(have_s), set(have_r))
        decode_s, decode_ok = 0.0, False
        if snap is not None:
            hs, hr = snap
            if symbols_src:
                avail = {j: unpack_repair(view["src"][j][1], expect=codec)[1] for j in hs}
            else:
                missing = set(range(n_src)) - hs
                erased = erased_symbols(lay, missing)
                src_syms = to_symbols(lay, [view["src"][i][1] if i in hs else bytes(sizes[i])
                                            for i in range(n_src)])
                avail = {i: sy for i, sy in enumerate(src_syms) if i not in erased}
            avail.update({lay.k + j: unpack_repair(view["rep"][j][1], expect=codec)[1] for j in hr})
            t = time.perf_counter()
            dec = Coder(lay.k, 0, codec).decode(avail)
            decode_s = time.perf_counter() - t
            decode_ok = dec is not None and from_symbols(lay, dec) == truth
        out[b] = dict(src_done=src_done, ready=ready, decode_s=decode_s, decode_ok=decode_ok)
    return out


def recovered_by(timings, D, include_decode):
    """Blocks recovered by deadline D from block_timings()."""
    n = 0
    inf = float("inf")
    for x in timings.values():
        # never-complete / never-decodable stay unrecovered even at D = inf
        if x["src_done"] < inf and x["src_done"] <= D:
            n += 1
        elif (x["decode_ok"] and x["ready"] < inf
              and x["ready"] + (x["decode_s"] if include_decode else 0.0) <= D):
            n += 1
    return n


def reconstruct_at(meta, blocks, D, rungs_used=None, t0_of=None):
    """loopback_fec.reconstruct, with the deadline rule in place of simulated drop.

    rungs_used: decode with repair from rungs 1..rungs_used only (0 = source only).
    Default: every rung the subscriber took. Because the subscriber recorded every
    rung it took, one run answers "what would fewer rungs have recovered" for the
    same arrivals. A late source object counts as erased; repair that arrived in
    time can cover it, exactly as it covers a source object that never arrived.

    t0 -- the block's first arrival -- is taken over ALL objects the subscriber
    received for the block, whatever rungs_used is: the deadline is anchored to
    when the block started arriving, not to which repair is being counted."""
    take = meta["take"] if rungs_used is None else rungs_used
    rows, ok = [], 0
    for b in measured(meta):
        truth = make_block(meta["seed"], b, meta["objects"], meta.get("gop", 0))
        view = blocks.get(b, {"src": {}, "rep": {}})
        ts = [t for t, _ in view["src"].values()] + [t for t, _ in view["rep"].values()]
        if not ts:
            rows.append(dict(block=b, ok=False, why="nothing arrived", lost=len(truth),
                             late=0, rep_in_time=0, offsets=[]))
            continue
        t0 = t0_of.get(b, min(ts)) if t0_of is not None else min(ts)
        allowed = set()
        for sl in rung_slices(layout_block([len(o) for o in truth], SPAN).k, meta["rungs"])[:take]:
            allowed.update(sl)
        if meta.get("src_symbols"):
            good, row = _reconstruct_symbols(meta, b, view, t0, D, allowed)
            ok += good
            row["offsets"] = sorted(t - t0 for t in ts)
            rows.append(row)
            continue
        src = {o: p for o, (t, p) in view["src"].items() if t - t0 <= D}
        reps = {j: p for j, (t, p) in view["rep"].items() if t - t0 <= D and j in allowed}
        late = sum(1 for t, _ in view["src"].values() if t - t0 > D)

        sizes = [len(o) for o in truth]          # needed only for the no-repair case below
        codec = repair_codec(next(iter(reps.values()))) if reps else None
        if reps:                                 # normal path: layout comes off the wire
            sizes, _ = unpack_repair(next(iter(reps.values())), expect=codec)
        lay = layout_block(sizes, SPAN)
        missing = set(range(len(sizes))) - set(src)
        erased = erased_symbols(lay, missing)
        decode_s, decode_bad = 0.0, False   # decode time on THIS host (post-run analysis)
        if not missing:
            # Whole source in time: byte-identical by direct comparison, no decode.
            good = [src[i] for i in range(len(truth))] == truth
        elif lay.k - len(erased) + len(reps) < lay.k:
            good = False                         # fewer than k symbols: cannot decode
        else:
            placeholder = [src.get(i, bytes(sizes[i])) for i in range(len(sizes))]
            src_syms = to_symbols(lay, placeholder)
            avail = {i: s for i, s in enumerate(src_syms) if i not in erased}
            avail.update({lay.k + j: unpack_repair(p, expect=codec)[1] for j, p in reps.items()})
            t_dec = time.perf_counter()
            out = Coder(lay.k, 0, codec).decode(avail)
            decode_s = time.perf_counter() - t_dec
            good = out is not None and from_symbols(lay, out) == truth
            decode_bad = not good           # k symbols in hand but not the seed's bytes
        ok += good
        rows.append(dict(block=b, ok=good, lost=len(missing), late=late, decode_s=decode_s,
                         decode_bad=decode_bad, codec=codec,
                         never=len(truth) - len(view["src"]), erased=len(erased),
                         k=lay.k, rep_in_time=len(reps), rep_total=len(view["rep"]),
                         offsets=sorted(t - t0 for t in ts)))
    return rows, ok


def spread_stats(blocks, meta, t0_of=None):
    """Per-block SOURCE COMPLETION TIME: from the first arrival of any object of the
    block (source or repair) to the arrival of its last source object. The quantity
    a deadline has to cover when nothing needs retransmitting. Only blocks whose
    source arrived complete are counted -- a block missing a source object has no
    completion time, and is reported as missing instead."""
    spreads, incomplete = [], 0
    for b in measured(meta):
        v = blocks.get(b)
        if not v or not v["src"]:
            incomplete += 1
            continue
        ts = [t for t, _ in v["src"].values()] + [t for t, _ in v["rep"].values()]
        t0 = t0_of.get(b, min(ts)) if t0_of is not None else min(ts)
        spreads.append(max(t for t, _ in v["src"].values()) - t0)
    spreads.sort()

    def pct(p):                                   # nearest-rank
        if not spreads:
            return float("nan")
        return spreads[max(0, -(-p * len(spreads) // 100) - 1)]
    return dict(n=len(spreads), incomplete=incomplete, p50=pct(50), p95=pct(95),
                p99=pct(99), max=spreads[-1] if spreads else float("nan"))


def path_loss(meta, blocks):
    """What the network actually lost, against what the seed says was sent.

    Source rides reliable streams, so a missing source object means the stream
    never completed -- not packet loss. Repair rides datagrams, so a missing repair
    object IS a lost datagram somewhere on publisher -> relay -> subscriber (or a
    receive-buffer drop on this host; run_demo.sh records the UDP counters that
    separate those)."""
    exp_rung = [0] * meta["take"]
    got_rung = [0] * meta["take"]
    src_missing = src_expected = 0
    for b in measured(meta):
        truth = make_block(meta["seed"], b, meta["objects"], meta.get("gop", 0))
        lay = layout_block([len(o) for o in truth], SPAN)
        slices = rung_slices(lay.k, meta["rungs"])
        v = blocks.get(b, {"src": {}, "rep": {}})
        # Arm B counts source in symbols (one per datagram); otherwise in objects.
        n_src = lay.k if meta.get("src_symbols") else len(truth)
        src_expected += n_src
        src_missing += n_src - len(v["src"])
        for r in range(meta["take"]):
            exp_rung[r] += len(slices[r])
            got_rung[r] += sum(1 for j in v["rep"] if j in slices[r])
    return dict(src_missing=src_missing, src_expected=src_expected,
                repair_expected=exp_rung, repair_received=got_rung)


def _pcts(xs):
    xs = sorted(xs)
    if not xs:
        return "n=0"
    at = lambda p: xs[max(0, -(-p * len(xs) // 100) - 1)]  # noqa: E731  nearest-rank
    return f"p50 {1e3*at(50):.1f} p95 {1e3*at(95):.1f} max {1e3*xs[-1]:.1f}"


def block_clock(meta, recs, blocks, clock, sends=None, base_window=None):
    """When each measured block's deadline clock starts, in this subscriber's clock.

    first-any  the block's first arrival of any object (the milestone-3 rule as
               first written). Taking repair moves it earlier.
    first-src  the block's first source arrival. Diagnostic only.
    send       the publisher's production time of the block's first source object,
               mapped to this subscriber's clock by the base delay: base = min over
               the run of (arrival - production) over every object received. The
               two monotonic clocks differ by an unknown constant; the base absorbs
               it, and everything above the fastest object's one-way delay counts
               (LEDBAT's trick -- no clock sync). Independent of the rungs taken.
    send-last  as send, but from the production of the block's LAST source object (or
               symbol): when the block is complete at the publisher. With spread
               emission (default since 2026-10-05) the first object is ~183 ms earlier;
               with burst emission (before) the two clocks differ by ~1 ms.

    base_window (seconds, send clock only): instead of one base for the whole run,
    each block's base is the min over objects PRODUCED within +-base_window/2 of the
    block's first source object (centered; this is post-hoc analysis). It absorbs a
    path change partway through a run. The cost: a queue that persists for longer
    than about half the window raises the base with it and disappears from the
    measurement.
    """
    win = measured(meta)
    if clock == "first-any":
        return {b: min([t for t, _ in blocks[b]["src"].values()] +
                       [t for t, _ in blocks[b]["rep"].values()])
                for b in win if blocks.get(b) and (blocks[b]["src"] or blocks[b]["rep"])}, {}
    if clock == "first-src":
        return {b: min(t for t, _ in blocks[b]["src"].values())
                for b in win if blocks.get(b) and blocks[b]["src"]}, {}
    assert clock in ("send", "send-last") and sends is not None, "--clock send needs --sends"
    S = sends["sends"]
    owd = [(g, tr, t - S[(tr, g, o)][0]) for tr, g, o, _m, t, _p in recs
           if (tr, g, o) in S and S[(tr, g, o)][0] is not None]
    base = min(d for _, _, d in owd)
    half = win.start + len(win) // 2
    # (a subscriber that stalled has no data in the second half: no drift estimate)
    base_1 = min((d for g, _, d in owd if win.start <= g < half), default=None)
    base_2 = min((d for g, _, d in owd if half <= g < win.stop), default=None)
    t0_of = {b: S[(SOURCE_TRACK, b, 0)][0] + base for b in win if (SOURCE_TRACK, b, 0) in S}
    if clock == "send-last":
        last = {}
        for (tr, g, o), v in S.items():
            if tr == SOURCE_TRACK and g in t0_of and v[0] is not None:
                last[g] = max(last.get(g, v[0]), v[0])
        t0_of = {b: last[b] + base for b in t0_of}
    drift = None if base_1 is None or base_2 is None else base_2 - base_1
    info = dict(base_s=base, base_drift_s=drift, per_track={}, pub={})
    if base_window:
        import bisect
        by_t = sorted((S[(tr, g, o)][0], t - S[(tr, g, o)][0]) for tr, g, o, _m, t, _p in recs
                      if (tr, g, o) in S and S[(tr, g, o)][0] is not None)
        ts = [x for x, _ in by_t]
        wbase = {}
        for b in list(t0_of):
            tp = S[(SOURCE_TRACK, b, 0)][0]
            lo = bisect.bisect_left(ts, tp - base_window / 2)
            hi = bisect.bisect_right(ts, tp + base_window / 2)
            wbase[b] = min(d for _, d in by_t[lo:hi])
            t0_of[b] = tp + wbase[b]
        shift = [wbase[b] - base for b in wbase]
        info["window_base_shift_s"] = dict(max=max(shift), median=sorted(shift)[len(shift) // 2])
        say(f"  clock=send, base per {base_window:g} s window: above the whole-run base by "
            f"median {1e3*sorted(shift)[len(shift)//2]:.2f} ms, max {1e3*max(shift):.2f} ms")
    say(f"  clock=send: base delay (offset-absorbing) {base:.6f}s; base in 2nd half - 1st half "
        f"of the measured window = "
        f"{'n/a (no data in one half)' if drift is None else f'{1e3*drift:+.2f} ms'} (clock drift + path change)")
    tracks = sorted({tr for _, tr, _ in owd}, key=lambda n: (n != SOURCE_TRACK, n))
    for tr in tracks:
        q = [d - base for g, t2, d in owd if t2 == tr and g in win]
        info["per_track"][tr] = q
        say(f"    {tr:4} one-way delay above base, per object (ms): {_pcts(q)}")
    for tr in tracks:
        ks = [k for k in S if k[0] == tr and k[1] in win and None not in S[k]]
        qd = [S[k][1] - S[k][0] for k in ks]
        wr = [S[k][2] - S[k][1] for k in ks]
        say(f"    pub {tr:4} produce->hand (lane queue) {_pcts(qd)} | hand->done (transport write) {_pcts(wr)}")
    leads = []
    for b in win:
        rp = [v[0] for k, v in S.items() if k[1] == b and k[0] != SOURCE_TRACK and v[0] is not None]
        if rp and (SOURCE_TRACK, b, 0) in S:
            leads.append(S[(SOURCE_TRACK, b, 0)][0] - min(rp))
    if leads:
        say(f"    pub: first repair produced BEFORE first source object by {_pcts(leads)}"
            f"  (negative = after)")
    return t0_of, info


def report(meta, recs, deadlines, json_path=None, clock="first-any", sends=None,
           base_window=None):
    annotate(meta, recs)
    blocks = block_view(recs)
    t0_of, clock_info = block_clock(meta, recs, blocks, clock, sends, base_window)
    sp = spread_stats(blocks, meta, t0_of)
    pl = path_loss(meta, blocks)
    win = measured(meta)
    say(f"\n  {meta['label']}: take={meta['take']} rungs={meta['rungs']}  "
        f"measured blocks {win.start}..{win.stop - 1} ({meta['blocks']}) x {meta['objects']}, "
        f"after {meta.get('warmup', 0)} warm-up")
    say(f"  source completion per block (clock={clock} -> last source object), ms: "
        f"p50={1e3*sp['p50']:.1f} p95={1e3*sp['p95']:.1f} p99={1e3*sp['p99']:.1f} "
        f"max={1e3*sp['max']:.1f}  (n={sp['n']}, incomplete={sp['incomplete']})")
    rep = "  ".join(f"rep{r+1} {g}/{e} ({100*(e-g)/e:.2f}% lost)" if e else f"rep{r+1} -"
                    for r, (g, e) in enumerate(zip(pl["repair_received"], pl["repair_expected"])))
    unit = "symbols" if meta.get("src_symbols") else "objects"
    say(f"  real-path loss: src missing {pl['src_missing']}/{pl['src_expected']} {unit}   "
        f"repair datagrams: {rep}")
    results = dict(meta=meta, clock=clock,
                   clock_info={k: v for k, v in clock_info.items() if k != "per_track"},
                   spread=sp, path_loss=pl, at={})
    for D in deadlines:
        results["at"][str(D)] = {}
        say(f"  D={1e3*D:g} ms:")
        for used in range(0, meta["take"] + 1):
            rows, ok = reconstruct_at(meta, blocks, D, rungs_used=used, t0_of=t0_of)
            late = sum(r["late"] for r in rows)
            never = sum(r.get("never", 0) for r in rows)
            missed = sum(1 for r in rows if r["lost"] > 0)
            rep_in = sum(r["rep_in_time"] for r in rows)
            what = "source only     " if used == 0 else f"src + rep1..rep{used}"
            say(f"    {what}  RECOVERED {ok:>3}/{meta['blocks']}   blocks with a source object "
                f"missing/late: {missed:>3}   src late={late} never={never}   repair in time {rep_in}")
            results["at"][str(D)][used] = dict(
                recovered=ok, blocks_missing_source=missed, src_late=late, src_never=never,
                rows=[{k: v for k, v in r.items() if k != "offsets"} for r in rows])
    if json_path:
        with open(json_path, "w") as f:
            json.dump(results, f, indent=1, default=str)
    return results


def run_analyze(a):
    with open(a.raw, "rb") as f:
        d = pickle.load(f)
    print_modes(d["meta"], d["recs"])
    sends = None
    if a.sends:
        with open(a.sends, "rb") as f:
            sends = pickle.load(f)
    report(d["meta"], d["recs"], a.deadlines, a.json, clock=a.clock, sends=sends,
           base_window=a.base_window)


# -------------------------------------------------------------------------- main

def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sp = p.add_subparsers(dest="role", required=True)

    def common(q):
        # OpenMoQ's public relays: GTM routes the name to the nearest one (issue #1).
        # Raw QUIC (moqt://) for pub; sub also takes https://lb.moqx.akaleapi.net/moq-relay.
        q.add_argument("--relay", default="moqt://lb.moqx.akaleapi.net:443/moq-relay")
        q.add_argument("--draft", type=parse_draft_spec, default=16)
        q.add_argument("--insecure", action="store_true", help="skip TLS verification of the relay")
        q.add_argument("--ns", required=True, help="per-run namespace, one token")
        q.add_argument("--seed", type=int, default=7)
        q.add_argument("--blocks", type=int, default=10)
        q.add_argument("--warmup", type=int, default=0,
                       help="unmeasured blocks sent before the measured ones (groups 0..warmup-1)")
        q.add_argument("--objects", type=int, default=12)
        q.add_argument("--gop", type=int, default=0,
                       help="0: an I-frame-sized object leads every block (block = GOP). N > 0: "
                            "one media timeline, an I-frame every N objects of the stream, cut "
                            "into blocks of --objects regardless (short FEC blocks)")
        q.add_argument("--rungs", type=rungs_arg, default=[0.15, 0.15, 0.15])
        q.add_argument("--debug", action="store_true")

    q = sp.add_parser("pub")
    common(q)
    q.add_argument("--gap", type=float, default=0.2, help="seconds between blocks")
    q.add_argument("--gate-file", required=True)
    q.add_argument("--keepalive", type=float, default=0, metavar="SEC",
                   help="QUIC keep-alive (PING) interval, so a session idling until the gate opens isn't "
                        "dropped on the relay's idle timeout; 0 (default) = off, as before")
    q.add_argument("--gop-frames", type=int, default=0, metavar="N",
                   help="with --media: frames per GOP, a multiple of --objects; each GOP is one MoQ "
                        "group of N / --objects blocks (default: --objects, a block per group)")
    q.add_argument("--media", default="", metavar="MP4",
                   help="Shaka phase 2: real LOC video frames from this mp4 as the source objects, one GOP "
                        "(--objects frames, keyframe first) per block, and an MSF catalog track")
    q.add_argument("--linger", type=float, default=10.0, help="stay connected after the last block")
    q.add_argument("--out", default="results")
    q.add_argument("--abrupt-exit", action="store_true",
                   help="leave after the linger without ending tracks or withdrawing the "
                        "namespace (the pre-2026-10-01 behaviour, kept to reproduce it)")
    q.add_argument("--source-mapping", choices=["group", "object", "datagram", "symbol-object",
                                                "symbol-subgroup"],
                   default="group",
                   help="milestone 4: group = arm A (stream per group), object = arm C (stream "
                        "per object), datagram = arm B (one symbol per datagram), "
                        "symbol-object = arm D (one symbol per stream), symbol-subgroup = arm D2 "
                        "(each source object's symbols on one stream)")
    q.add_argument("--repair-mapping", choices=["datagram", "object", "group"], default="datagram",
                   help="repair rungs on datagrams (default), one stream per object (arm D), or "
                        "one stream per rung per block (arm D2)")
    q.add_argument("--repair-ids", choices=["global", "per-rung"], default="global",
                   help="repair object ids: the global repair index (default), or 0..n-1 per "
                        "rung per group with the global index in the payload (INDEXED form), so "
                        "a relay cache sees every rung's group start at object 0")
    q.add_argument("--repair-eog", action="store_true",
                   help="mark each block's last repair object END_OF_GROUP (datagram bit 0x02), so "
                        "a relay cache knows the group is complete (no tail fetch upstream)")
    q.add_argument("--repair-group-per-symbol", action="store_true",
                   help="each repair symbol its own one-object group (group = block*256 + global "
                        "index, object 0; index also in the payload) -- for relays that keep only "
                        "object 0 of a datagram group. Not combinable with --repair-eog/catch-up")
    q.add_argument("--pace-repair", type=float, default=0.0, metavar="FRAC",
                   help="spread each rung's repair for a block evenly over FRAC of the block period "
                        "after the source is out, instead of back to back (0 = burst, the default)")
    q.add_argument("--pace-source", type=float, default=0.0, metavar="FRAC",
                   help="--source-mapping datagram only: spread the symbols of each source object "
                        "evenly over FRAC of the object's interval (spread x gap / objects; the "
                        "block period with --spread-source 0) instead of back to back (0 = burst, "
                        "the default)")
    q.add_argument("--repair-catch-up", action="store_true",
                   help="a rung re-subscribed mid-run gets the repair of the last block "
                        "written, as a first subscription does (datagram rungs; live adaptation)")
    q.add_argument("--mirror-mapping", choices=["none", "datagram", "object"], default="none",
                   help="also offer the symbol family as track 'sym' (datagram = arm B's, "
                        "object = arm D's), so one run can serve both families")
    q.add_argument("--prio-src", type=int, default=128, help="publisher priority, src (lower = more urgent)")
    q.add_argument("--prio-rep", type=int, default=128, help="publisher priority, repair tracks")
    q.add_argument("--qlog-dir", default=None, help="picoquic qlog for this connection (diagnostic)")
    q.add_argument("--serve-fetch", action=argparse.BooleanOptionalAction, default=True,
                   help="answer FETCH (standalone and joining) for the source, mirror and rung tracks, "
                        "subscribed or not, from the schedule (repair by FETCH; the relay sends upstream "
                        "what it doesn't hold). On by default since 2026-10-24; --no-serve-fetch refuses "
                        "every FETCH, as before")
    q.add_argument("--fetch-window", type=float, default=30.0, metavar="S",
                   help="seconds of blocks FETCH can reach (default 30); older: FETCH_OK with an End "
                        "of Unknown Range marker")
    q.add_argument("--spread-source", type=float, default=1.0, metavar="FRAC",
                   help="write each block's source objects spread over FRAC of the block period: "
                        "object i (or the symbols starting in it) at slot + i*FRAC*gap/n. Default "
                        "1.0 since 2026-10-05; 0 = all at once at the slot (the behaviour before)")

    q = sp.add_parser("sub")
    common(q)
    q.add_argument("--take", type=int, required=True, help="subscribe to rungs 1..take")
    q.add_argument("--label", required=True)
    q.add_argument("--duration", type=float, required=True, help="seconds to stay subscribed")
    q.add_argument("--subscribe-timeout", type=float, default=20.0)
    q.add_argument("--prio-src", type=int, default=None,
                   help="subscriber priority for src (default: omitted from SUBSCRIBE)")
    q.add_argument("--prio-rep", type=int, default=None, help="subscriber priority, repair tracks")
    q.add_argument("--out", default="results")
    q.add_argument("--deadlines", type=lambda s: [float(x) for x in s.split(",")], default=[])
    q.add_argument("--qlog-dir", default=None, help="picoquic qlog for this connection (diagnostic)")
    q.add_argument("--gap", type=float, default=0.2, help="block period (the publisher's --gap)")
    q.add_argument("--spread-source", type=float, default=1.0,
                   help="the publisher's --spread-source (the 'due' adaptation rule needs it)")
    q.add_argument("--repair-group-per-symbol", action="store_true",
                   help="the publisher's --repair-group-per-symbol: map repair group ids back to blocks")
    q.add_argument("--adapt-detect", choices=["due", "hole"], default="hole",
                   help="how --adaptive judges a block lossy (see adapt()); default hole since "
                        "2026-10-06 (due was the default 2026-10-05, and the only rule before)")
    q.add_argument("--adaptive", action="store_true",
                   help="start with the source only; subscribe rungs 1..take when a block is "
                        "missing source symbols, unsubscribe after 10 clean blocks (arm B)")
    q.add_argument("--stop-decoded", action="store_true",
                   help="STOP_SENDING a block's remaining streams once it holds k symbols")
    q.add_argument("--source-track", default=SOURCE_TRACK,
                   help="source track to subscribe: src, or sym for the mirror family")

    q = sp.add_parser("analyze")
    q.add_argument("--raw", required=True)
    q.add_argument("--deadlines", type=lambda s: [float(x) for x in s.split(",")], required=True)
    q.add_argument("--json")
    q.add_argument("--clock", choices=["first-any", "first-src", "send", "send-last"], default="first-any")
    q.add_argument("--sends", help="pub.sends.pkl from the same run (for --clock send)")
    q.add_argument("--base-window", type=float, default=None,
                   help="send clock: base delay per centered window of this many seconds")

    a = p.parse_args()
    if a.role == "analyze":
        return run_analyze(a)
    if a.role == "sub" and not 0 <= a.take <= len(a.rungs):
        p.error(f"--take must be 0..{len(a.rungs)}")
    set_log_level(logging.DEBUG if a.debug else logging.WARNING)
    if not a.debug:
        keep_control_lines()
    asyncio.run(run_pub(a) if a.role == "pub" else run_sub(a))


def keep_control_lines():
    """Let through, in full, aiomoqt's INFO lines for SERVER_SETUP (the relay's
    IMPLEMENTATION string, i.e. what is serving us) and every SUBSCRIBE_OK, sent or
    received (its parameters and track extensions, e.g. MAX_CACHE_DURATION): a handful
    of lines per run. Everything else stays at WARNING. aiomoqt logs no INFO on the
    object path we use, so raising its level costs nothing per object."""
    from aiomoqt.utils import logger as amlog

    class ControlLines(logging.Filter):
        def filter(self, r):
            if r.levelno >= logging.WARNING:
                return True
            msg = r.getMessage()
            return r.levelno == logging.INFO and ("ServerSetup" in msg or "SubscribeOk" in msg)

    if amlog._handler is not None:
        amlog._handler.addFilter(ControlLines())
        set_log_level(logging.INFO)


if __name__ == "__main__":
    main()
