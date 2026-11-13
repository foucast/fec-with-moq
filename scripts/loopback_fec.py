"""Source track + a ladder of repair tracks over real MoQT. Loopback, no relay.

The publisher serves `src` and repair rungs `rep1..repN`. The subscriber takes
`src` plus rungs 1..--take, SELECTED PURELY BY WHICH TRACKS IT SUBSCRIBES TO. It
then discards chosen source objects and reconstructs from whatever repair arrived.

Milestone 2b's single repair track is the one-rung case:  --rungs 0.30 --take 1

⚠️ Loss is SIMULATED AT THE RECEIVER. Every object genuinely traverses MoQT and the
decode is real, but which source objects are "lost" is chosen, not produced by a
network. The drop sequence is seeded, so runs with different --take see exactly
the same losses on exactly the same blocks. Real network loss is milestone 3.
"""
import argparse
import asyncio
import logging
import random
import sys
import time
from collections import defaultdict
from functools import partial

sys.path.insert(0, "src")

from aiomoqt.client import MOQTClient  # noqa: E402
from aiomoqt.server import MOQTServer  # noqa: E402
from aiomoqt.track import SubscribedTrack  # noqa: E402
from aiomoqt.types import MOQTMessageType, ObjectStatus, parse_draft_spec  # noqa: E402
from aiomoqt.utils.logger import set_log_level  # noqa: E402

from fecmoq.codec import Coder  # noqa: E402
from fecmoq.moq import (NAMESPACE, SOURCE_TRACK, BlockSchedule, RepairTrack, SourceTrack,  # noqa: E402
                        make_block, repair_track_name, rung_slices)
from fecmoq.symbols import SPAN, erased_symbols, from_symbols, layout_block, to_symbols  # noqa: E402
from fecmoq.wire import unpack_repair  # noqa: E402


async def on_subscribe(session, msg, a):
    name = (msg.track_name or b"").decode()
    # One schedule for all of this origin's tracks: repair follows its source.
    if not hasattr(a, "schedule"):
        a.schedule = BlockSchedule(seed=a.seed, n_blocks=a.blocks, n_objects=a.objects,
                                   gap_s=a.gap, rung_fracs=a.rungs)
    common = dict(seed=a.seed, n_blocks=a.blocks, n_objects=a.objects, gap_s=a.gap,
                  schedule=a.schedule)
    rung_names = {repair_track_name(r): r for r in range(1, len(a.rungs) + 1)}
    if name == SOURCE_TRACK:
        track = SourceTrack(session, **common)
    elif name in rung_names:
        track = RepairTrack(session, rung=rung_names[name], rung_fracs=a.rungs, **common)
    else:
        return
    ok = session.subscribe_ok(request_msg=msg)
    track.track_alias = ok.track_alias
    track._generating = True
    await track.generate(session, ok.track_alias)


async def run_server(a):
    server = MOQTServer(host="localhost", port=a.port, certificate=a.cert, private_key=a.key,
                        path="/", use_quic=True, supported_drafts=a.draft)
    server.register_handler(MOQTMessageType.SUBSCRIBE, partial(on_subscribe, a=a))
    return await server.serve()


async def run_subscriber(a, got_src, got_rep, per_rung):
    # Only NORMAL objects are data. aiomoqt delivers status objects -- e.g. an
    # END_OF_GROUP marker after each group's last object -- through the same
    # callback. Storing those as data was a real bug in milestone 2b's first run.
    def _data(msg):
        return getattr(msg, "status", ObjectStatus.NORMAL) == ObjectStatus.NORMAL

    def on_src(msg, consumed, now, group_id, subgroup_id=None):
        if _data(msg):
            got_src[group_id][msg.object_id] = bytes(msg.payload)

    def make_on_rep(rung):
        def on_rep(msg, consumed, now, group_id, subgroup_id=None):
            if _data(msg):
                got_rep[group_id][msg.object_id] = bytes(msg.payload)   # object_id = GLOBAL index
                per_rung[rung] += 1
        return on_rep

    client = MOQTClient("localhost", a.port, path="/", use_quic=True,
                        supported_drafts=a.draft, verify_tls=False)
    async with client.connect() as session:
        await session.client_session_init()
        # Repair rungs first, so no early repair datagram beats its handler.
        for r in range(1, a.take + 1):
            t = SubscribedTrack(session, namespace=NAMESPACE, trackname=repair_track_name(r),
                                on_object=make_on_rep(r))
            await t.subscribe()
        src = SubscribedTrack(session, namespace=NAMESPACE, trackname=SOURCE_TRACK, on_object=on_src)
        await src.subscribe()
        await asyncio.sleep(a.blocks * a.gap + a.settle)


def reconstruct(a, got_src, got_rep):
    rng = random.Random(a.seed + 99)
    rows, ok_blocks = [], 0
    for b in range(a.blocks):
        truth = make_block(a.seed, b, a.objects)
        sizes = [len(o) for o in truth]            # needed only for the no-repair case below
        reps = got_rep.get(b, {})
        if reps:                                   # normal path: layout comes off the wire
            sizes, _ = unpack_repair(next(iter(reps.values())))
        lay = layout_block(sizes, SPAN)

        arrived = dict(got_src.get(b, {}))
        for i in rng.sample(range(len(sizes)), min(a.drop, len(sizes))):  # SIMULATED loss
            arrived.pop(i, None)
        missing = set(range(len(sizes))) - set(arrived)
        erased = erased_symbols(lay, missing)

        placeholder = [arrived.get(i, bytes(sizes[i])) for i in range(len(sizes))]
        src_syms = to_symbols(lay, placeholder)
        avail = {i: s for i, s in enumerate(src_syms) if i not in erased}
        avail.update({lay.k + j: unpack_repair(p)[1] for j, p in reps.items()})

        out = Coder(lay.k, 0).decode(avail) if len(avail) >= lay.k else None
        good = out is not None and from_symbols(lay, out) == truth
        ok_blocks += good
        rows.append((b, "RECOVERED" if good else "FAILED", len(missing), len(erased), len(reps)))
    return rows, ok_blocks


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=4450)
    p.add_argument("--cert", default="certs/cert.pem")
    p.add_argument("--key", default="certs/key.pem")
    p.add_argument("--draft", type=parse_draft_spec, default=16)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--blocks", type=int, default=10)
    p.add_argument("--objects", type=int, default=12)
    p.add_argument("--rungs", type=lambda s: [float(x) for x in s.split(",")], default=[0.15, 0.15, 0.15],
                   help="per-rung repair, as fractions of k (additive). Default 3 rungs of 15%%")
    p.add_argument("--take", type=int, default=3, help="subscribe to rungs 1..take (0 = source only)")
    p.add_argument("--drop", type=int, default=2, help="source objects discarded per block (simulated)")
    p.add_argument("--gap", type=float, default=0.05)
    p.add_argument("--settle", type=float, default=1.5)
    p.add_argument("--quiet", action="store_true", help="summary lines only")
    a = p.parse_args()
    if not 0 <= a.take <= len(a.rungs):
        p.error(f"--take must be 0..{len(a.rungs)}")
    set_log_level(logging.WARNING)

    got_src, got_rep, per_rung = defaultdict(dict), defaultdict(dict), defaultdict(int)
    server = await run_server(a)
    t0 = time.time()
    try:
        await run_subscriber(a, got_src, got_rep, per_rung)
    finally:
        server.close()

    rows, ok = reconstruct(a, got_src, got_rep)
    cum = sum(a.rungs[: a.take])
    if not a.quiet:
        print(f"draft {a.draft}  {a.blocks} blocks x {a.objects} objects  rungs={a.rungs}  "
              f"take={a.take} (~{cum:.0%} of k)  drop={a.drop}/block simulated  [{time.time()-t0:.1f}s]\n")
        print(f"  {'blk':>3}  {'result':<10} {'lost_obj':>8} {'erased_sym':>10} {'repair_rx':>9}")
        for b, r, lo, er, rx in rows:
            print(f"  {b:>3}  {r:<10} {lo:>8} {er:>10} {rx:>9}")
        print()
    total_src = sum(len(v) for v in got_src.values())
    rung_rx = "  ".join(f"rep{r}={per_rung.get(r, 0)}" for r in range(1, len(a.rungs) + 1))
    print(f"  take={a.take} (~{cum:.0%} of k): RECOVERED {ok}/{a.blocks}   "
          f"rx: src={total_src}  {rung_rx}")


if __name__ == "__main__":
    asyncio.run(main())
