"""FETCH through a relay, by range or joining, checked object by object (synthetic runs only).

    python scripts/fetch_range.py --relay URL --ns NS --now G \
        [--range TRACK:BACK0-BACK1 ...] [--joining TRACK:N ...] [publisher options]

--now G is the group in production when the probe starts (the caller computes it). Each
--range FETCHes groups G-BACK0 .. G-BACK1 (whole groups) in ONE standalone FETCH; BACK0 == BACK1
is a single group. Each --joining SUBSCRIBEs to TRACK (Latest Object) and sends a relative joining
FETCH N groups back. Every object that comes back is compared with what the publisher writes for
that Location (fecmoq.moq.track_objects, from the seed). A FETCH answer carries no status objects
(none exist in the FETCH format from draft 16); any that arrive are counted.
The publisher options (--source-mapping, --repair-ids, --repair-group-per-symbol, --repair-eog,
--seed, --objects, --rungs, --coder) must match the run's.

Per FETCH it prints the reply (FETCH_OK or the error), data objects byte-identical / expected,
status objects received (expected 0), End of Range markers (the publisher's window, or the relay's), the time to the
reply and to the end of the fetch stream.
"""
import argparse
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from aiomoqt.client import MOQTClient  # noqa: E402
from aiomoqt.types import FilterType, ObjectStatus  # noqa: E402
from aiomoqt.utils.url import parse_relay_url  # noqa: E402

from fecmoq.moq import GROUP_STRIDE, BlockSchedule, FetchSpec, StreamMapping, track_objects  # noqa: E402


def spec_for(track, a):
    if track == "src":
        return FetchSpec(kind="source", source_mapping=a.source_mapping)
    return FetchSpec(kind="repair", mapping=StreamMapping.DATAGRAM, rung=int(track[3:]),
                     per_rung_ids=a.repair_ids == "per-rung",
                     group_per_symbol=a.repair_group_per_symbol, end_of_group=a.repair_eog)


def expected(sch, spec, g0, g1):
    """{(group, object): (payload, status)} the publisher wrote in groups g0..g1."""
    if spec.kind == "repair" and spec.group_per_symbol:
        blocks = range(g0 // GROUP_STRIDE, g1 // GROUP_STRIDE + 1)
    else:
        blocks = range(g0, g1 + 1)
    return {(g, o): (p, st) for b in blocks if b >= 0
            for g, o, _, p, st in track_objects(sch, spec, b) if g0 <= g <= g1}


async def main(a):
    os.environ.setdefault("FECMOQ_CODER", a.coder)
    sch = BlockSchedule(seed=a.seed, n_blocks=1 << 30, n_objects=a.objects, gap_s=0.2, rung_fracs=a.rungs)
    u = parse_relay_url(a.relay)
    from urllib.parse import urlparse
    got = {}                     # request id -> [(group, object, payload, status, end_of_range)]
    async with MOQTClient(u.host, u.port, path=u.path or urlparse(a.relay).path or "", use_quic=u.use_quic,
                          supported_drafts=a.draft, verify_tls=True).connect() as s:
        await s.client_session_init()

        def on_obj(msg, size, now, request_id):
            got.setdefault(request_id, []).append(
                (msg.group_id, msg.object_id, bytes(msg.payload or b""),
                 getattr(msg, "status", ObjectStatus.NORMAL), getattr(msg, "end_of_range", None)))
        s.on_fetch_object = on_obj

        async def one(label, track, g0, g1, send):
            t0 = time.monotonic()
            msg = send()
            rid = msg.request_id
            try:
                reply = await asyncio.wait_for(s._await_response(rid), 10)
            except asyncio.TimeoutError:
                reply = "TIMEOUT"
            except Exception as e:          # noqa: BLE001
                reply = f"ERROR({e})"
            t_reply = 1e3 * (time.monotonic() - t0)
            kind = type(reply).__name__ if not isinstance(reply, str) else reply
            done = None
            if "Ok" in kind:
                try:
                    done = await s.await_fetch_done(rid, timeout=10)
                except Exception as e:      # noqa: BLE001
                    done = type(e).__name__
            t_done = 1e3 * (time.monotonic() - t0)
            objs = got.get(rid, [])
            markers = [(g, o, e) for g, o, _, _, e in objs if e is not None]
            data = [(g, o, p) for g, o, p, st, e in objs if e is None and st == ObjectStatus.NORMAL]
            stat = [(g, o, st) for g, o, p, st, e in objs if e is None and st != ObjectStatus.NORMAL]
            if g0 is None:                  # joining: the range is what came back
                gs = [g for g, *_ in data + stat] or [0]
                g0, g1 = min(gs), max(gs)
            want = expected(sch, spec_for(track, a), g0, g1)
            want_data = {k: p for k, (p, st) in want.items() if st is None}
            same = sum(1 for g, o, p in data if want_data.get((g, o)) == p)
            wrong = len(data) - same
            extra = ""
            if "Ok" in kind:
                extra = (f" end_of_track={getattr(reply, 'end_of_track', None)} largest="
                         f"{getattr(reply, 'largest_group_id', None)}.{getattr(reply, 'largest_object_id', None)}")
            else:
                extra = f" {reply}"
            print(f"FETCH {label} {track} groups {g0}-{g1}: reply {kind}{extra}; data {same} byte-identical "
                  f"of {len(want_data)} expected ({wrong} wrong); status objects {len(stat)} (a FETCH carries none); "
                  f"end-of-range markers {markers or 0}; reply {t_reply:.0f} ms, done {done} {t_done:.0f} ms",
                  flush=True)

        for r in a.range:
            track, span = r.split(":")
            b0, b1 = (int(x) for x in span.split("-"))
            g0, g1 = max(0, a.now - b0), max(0, a.now - b1)
            if track != "src" and a.repair_group_per_symbol:   # blocks back -> their symbol groups
                g0, g1 = g0 * GROUP_STRIDE, g1 * GROUP_STRIDE + GROUP_STRIDE - 1
            await one("range", track, g0, g1, lambda: s.fetch(
                namespace=a.ns, track_name=track, start_group=g0, start_object=0,
                end_group=g1, end_object=0, wait_response=False))
        for j in a.joining:
            track, n = j.split(":")
            ok = await s.subscribe(namespace=a.ns, track_name=track, forward=1,
                                   filter_type=FilterType.LATEST_OBJECT, wait_response=True)
            sub_rid = getattr(ok, "request_id", None)
            print(f"SUBSCRIBE {track}: {type(ok).__name__} request_id={sub_rid} content_exists="
                  f"{getattr(ok, 'content_exists', None)} largest={getattr(ok, 'largest_group_id', None)}."
                  f"{getattr(ok, 'largest_object_id', None)}", flush=True)
            await one(f"joining-{n}", track, None, None, lambda: s.joining_fetch(
                sub_rid, joining_start=int(n), wait_response=False))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--relay", required=True)
    p.add_argument("--ns", required=True)
    p.add_argument("--draft", type=int, default=16)
    p.add_argument("--now", type=int, required=True, help="group in production now")
    p.add_argument("--range", action="append", default=[], metavar="TRACK:BACK0-BACK1")
    p.add_argument("--joining", action="append", default=[], metavar="TRACK:N")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--objects", type=int, default=12)
    p.add_argument("--rungs", type=lambda s: [float(x) for x in s.split(",")], default=[0.15] * 3)
    p.add_argument("--coder", default="isal")
    p.add_argument("--source-mapping", default="group", choices=["group", "object", "datagram"])
    p.add_argument("--repair-ids", choices=["global", "per-rung"], default="global")
    p.add_argument("--repair-group-per-symbol", action="store_true")
    p.add_argument("--repair-eog", action="store_true")
    asyncio.run(main(p.parse_args()))
