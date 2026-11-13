"""FETCH feasibility probe against the relay (milestone 4, step 4 prerequisite).

    python scripts/fetch_check.py --relay URL --ns NS --rungs 0.15,0.15,0.15 --seed 7 \
        --objects 12 --groups 40,5 [--tracks src,rep1] [--coder isal]

For each (track, group): one standalone FETCH of the whole group through the relay; reports
the reply (FETCH_OK / error), how many objects came back, how many are byte-identical to what
the seed says was published, and how long it took. Our publisher answers FETCH with
NOT_SUPPORTED (aiomoqt's default handler), so anything served came from the relay's cache.
"""
import argparse
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.dirname(__file__))
from aiomoqt.client import MOQTClient  # noqa: E402
from aiomoqt.utils.url import parse_relay_url  # noqa: E402

from fecmoq.codec import Coder  # noqa: E402
from fecmoq.moq import make_block, rung_slices  # noqa: E402
from fecmoq.symbols import SPAN, layout_block, to_symbols  # noqa: E402
from fecmoq.wire import pack_repair  # noqa: E402


def expected(track, g, a):
    """{object_id: payload} the publisher sent for this track and group."""
    objs = make_block(a.seed, g, a.objects)
    if track == "src":
        return dict(enumerate(objs))
    sizes = [len(o) for o in objs]
    lay = layout_block(sizes, SPAN)
    slices = rung_slices(lay.k, a.rungs)
    rep = Coder(lay.k, slices[-1].stop, a.coder).encode(to_symbols(lay, objs))
    r = int(track[3:])
    if a.repair_group_per_symbol:
        # repair symbol j of block g is group g*256 + j, object 0, indexed payload
        return {(g * 256 + j, 0): pack_repair(sizes, rep[j], a.coder, index=j) for j in slices[r - 1][:3]}
    if a.repair_ids == "per-rung":
        return {n: pack_repair(sizes, rep[j], a.coder, index=j) for n, j in enumerate(slices[r - 1])}
    return {j: pack_repair(sizes, rep[j], a.coder) for j in slices[r - 1]}


async def main(a):
    u = parse_relay_url(a.relay)
    got = {}
    from urllib.parse import urlparse
    async with MOQTClient(u.host, u.port, path=u.path or urlparse(a.relay).path or "", use_quic=u.use_quic,
                          supported_drafts=a.draft, verify_tls=True).connect() as s:
        await s.client_session_init()

        def on_obj(msg, size, now, request_id):
            got.setdefault(request_id, {})[msg.object_id] = (bytes(msg.payload), msg.group_id)
        s.on_fetch_object = on_obj
        raw = {}                         # --capture: stream id -> bytes as aiopquic delivered them
        if a.capture:
            # Record every incoming stream's bytes exactly as the QUIC layer hands them to
            # aiomoqt (after reassembly, before any MoQT parsing), to compare with the
            # wire (decrypted pcap) and with what aiomoqt decoded.
            orig_on_stream_data = s._on_stream_data

            def on_stream_data(stream_id, data, end_stream):
                raw.setdefault(stream_id, bytearray()).extend(data)
                return orig_on_stream_data(stream_id, data, end_stream)
            s._on_stream_data = on_stream_data
        fetches = []                     # (track, group, start, request id)
        if a.repair_group_per_symbol:
            # one FETCH per repair symbol group (first 3 of the rung per block), object 0 each
            for track in [t for t in a.tracks if t != "src"]:
                for b in a.groups:
                    for (G, _o), want in expected(track, b, a).items():
                        msg = s.fetch(namespace=a.ns, track_name=track, start_group=G, start_object=0,
                                      end_group=G, end_object=0, wait_response=False)
                        rid = msg.request_id
                        try:
                            reply = await asyncio.wait_for(s._await_response(rid), 10)
                        except Exception as e:     # noqa: BLE001
                            reply = f"ERROR({e})"
                        kind = type(reply).__name__ if not isinstance(reply, str) else reply
                        if "Ok" in kind:
                            try:
                                await s.await_fetch_done(rid, timeout=10)
                            except Exception:      # noqa: BLE001
                                pass
                        objs = got.get(rid, {})
                        p0 = objs.get(0, (None, None))[0]
                        print(f"FETCH {track} block {b} group {G}: reply {kind}; objects {len(objs)}; "
                              f"object 0 byte-identical {p0 == want}")
            return
        for track in a.tracks:
            for g in a.groups:
                t0 = time.monotonic()
                # --start-first: start at the lowest object id this track actually has in the
                # group (a repair rung's first global repair index) instead of 0.
                start = min(expected(track, g, a)) if a.start_first else 0
                msg = s.fetch(namespace=a.ns, track_name=track, start_group=g, start_object=start,
                              end_group=g, end_object=0, wait_response=False)
                rid = msg.request_id
                fetches.append((track, g, start, rid))
                try:
                    reply = await asyncio.wait_for(s._await_response(rid), 10)
                except asyncio.TimeoutError:
                    reply = "TIMEOUT"
                except Exception as e:          # noqa: BLE001  e.g. MOQTRequestError
                    reply = f"ERROR({e})"
                kind = type(reply).__name__ if not isinstance(reply, str) else reply
                done = None
                if "Ok" in kind:
                    try:
                        done = await s.await_fetch_done(rid, timeout=10)
                    except Exception as e:       # noqa: BLE001
                        done = f"{type(e).__name__}"
                dt = 1e3 * (time.monotonic() - t0)
                want = expected(track, g, a)
                objs = got.get(rid, {})
                same = sum(1 for o, (p, gg) in objs.items() if gg == g and want.get(o) == p)
                detail = "" if "Ok" in kind else f" {reply}"
                if a.verbose and objs:
                    for o in sorted(objs):
                        p_, gg = objs[o]
                        w = want.get(o)
                        print(f"   obj {o} group {gg}: got {len(p_)}B want {len(w) if w else '-'}B "
                              f"{'same' if w == p_ else ('PREFIX' if w and w.startswith(p_) else 'diff')}"
                              f"{'' if w == p_ or not w else f' first-diff@{next((i for i, (x, y) in enumerate(zip(p_, w)) if x != y), min(len(p_), len(w)))}'}")
                if a.verbose:
                    for o_, (p_, gg) in sorted(objs.items()):
                        w = want.get(o_)
                        if w and w != p_:
                            i = next(i for i, (x, y) in enumerate(zip(p_, w)) if x != y)
                            nd = sum(1 for x, y in zip(p_, w) if x != y)
                            z = sum(1 for x, y in zip(p_, w) if x != y and x == 0)
                            # is the wrong tail some OTHER object's bytes? check other objects of the block
                            src_of = next((f"obj{k2}@{w2.find(p_[i:i+64])}" for k2, w2 in want.items()
                                           if k2 != o_ and p_[i:i+64] and w2.find(p_[i:i+64]) >= 0), "-")
                            print(f"   obj {o_}: {nd} bytes differ from @{i}; {z} of them zero; "
                                  f"got[@{i}] {p_[i:i+8].hex()} want {w[i:i+8].hex()}; wrong bytes found in {src_of}")
                print(f"FETCH {track} group {g} from obj {start}: reply {kind}{detail}; objects {len(objs)} of "
                      f"{len(want)}; byte-identical {same}; fetch done {done}; {dt:.0f} ms", flush=True)
        if a.capture:
            import pickle
            os.makedirs(a.capture, exist_ok=True)
            out = dict(fetches=fetches, raw={k: bytes(v) for k, v in raw.items()},
                       decoded={rid: {o: (p, gg) for o, (p, gg) in objs.items()} for rid, objs in got.items()},
                       expected={(t, g): expected(t, g, a) for t, g, _s, _r in fetches},
                       args=vars(a))
            path = os.path.join(a.capture, "fetch_capture.pkl")
            with open(path, "wb") as fh:
                pickle.dump(out, fh)
            print(f"capture: {len(raw)} streams, {sum(len(v) for v in raw.values())} bytes -> {path}",
                  flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--relay", default="moqt://moqx-main.ci.openmoq.org:4433")
    p.add_argument("--ns", required=True)
    p.add_argument("--draft", type=int, default=16, help="MoQT draft (16 for moqx, 18 for MOQtail)")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--objects", type=int, default=12)
    p.add_argument("--rungs", type=lambda s: [float(x) for x in s.split(",")], default=[0.15] * 3)
    p.add_argument("--groups", type=lambda s: [int(x) for x in s.split(",")], required=True)
    p.add_argument("--tracks", type=lambda s: s.split(","), default=["src", "rep1"])
    p.add_argument("--coder", default="isal")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--repair-group-per-symbol", action="store_true",
                   help="the publisher's --repair-group-per-symbol: FETCH repair symbol groups (block*256 + j)")
    p.add_argument("--repair-ids", choices=["global", "per-rung"], default="global",
                   help="how the publisher numbered repair objects (relay_fec pub --repair-ids)")
    p.add_argument("--capture", default="",
                   help="directory: save each incoming stream's raw bytes (as aiopquic delivered "
                        "them), aiomoqt's decoded objects and the expected payloads, as a pickle")
    p.add_argument("--start-first", action="store_true",
                   help="start each FETCH at the track's first real object id in the group, not 0")
    asyncio.run(main(p.parse_args()))
