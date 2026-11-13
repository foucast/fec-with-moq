"""Edge-case probe through a relay (aiomoqt 0.11.1, the repo venv). Synthetic payloads only.
Checks how a relay handles object-id gaps, extensions, the datagram END_OF_GROUP bit
without a status object, session end, duplicate PUBLISH_NAMESPACE and PUBLISH. Run from the repo root:
    .venv/bin/python scripts/relay_edge_probe.py pub moqt://HOST:PORT tNS /tmp/gate --end clean   # node 1
    .venv/bin/python scripts/relay_edge_probe.py sub moqt://HOST:PORT tNS 30                      # node 2
    touch /tmp/gate                                                                               # node 1

  pub  RELAY NS GATEFILE [--draft 16] [--end clean|abrupt|nsdone|hang] [--groups 10]
       announces NS; serves on SUBSCRIBE:
         'g'  subgroup stream per group, object ids 0,1,3,7 (gaps), extensions on objects 1 and 3
         'e'  stream per group, ids 0..3, extensions on every object (even key int, odd key bytes)
         'd'  datagrams ids 0..4, last with the END_OF_GROUP type bit (0x02) and NO status object;
              object 1 carries extensions
       waits for GATEFILE, sends --groups groups 0.2 s apart, lingers 3 s, then --end:
         clean  PUBLISH_DONE per subscription, then PUBLISH_NAMESPACE_DONE, then close
         nsdone PUBLISH_NAMESPACE_DONE only (tracks not ended), stay 5 s, close
         abrupt close the connection without either
         hang   stay connected, sending nothing, until killed (kill -9 it to model a dead publisher)
  sub  RELAY NS DURATION [--draft 16]  subscribes g, e, d; prints every object and every PUBLISH_DONE
  dup  RELAY NS [--draft 16]           PUBLISH_NAMESPACE of NS from a second session; prints the reply
  push RELAY NS [--draft 16]           PUBLISH (push, forward=0) of NS/'p'; prints the reply, then closes
"""
import argparse
import asyncio
import os
import sys
import time

from aiomoqt.client import MOQTClient
from aiomoqt.messages.data import ObjectDatagram, SubgroupHeader
from aiomoqt.types import MOQTMessageType, ObjectStatus
from aiomoqt.utils.url import parse_relay_url

EXT = {0x40: 7, 0x41: b"x-probe"}       # even key: integer value; odd key: bytes


def say(*a):
    print(f"{time.time():.3f}", *a, flush=True)


def client(a):
    u = parse_relay_url(a.relay)
    return MOQTClient(u.host, u.port, path=u.path or "", use_quic=u.use_quic,
                      supported_drafts=a.draft, verify_tls=True)


def payload(tr, g, o):
    return (f"{tr}:{g}:{o}:".encode() + bytes(range(256)) * 2)[:300]


async def run_pub(a):
    subs = {}

    async def on_subscribe(s, msg):
        name = (msg.track_name or b"").decode()
        ok = s.subscribe_ok(request_msg=msg)
        subs[name] = (ok.track_alias, msg.request_id)
        say(f"UPSTREAM-SUBSCRIBE {name} alias={ok.track_alias} filter={getattr(msg, 'filter_type', None)} "
            f"sub_prio={getattr(msg, 'priority', None)} forward={getattr(msg, 'forward', None)}")

    async with client(a).connect() as s:
        await s.client_session_init()
        s.register_handler(MOQTMessageType.SUBSCRIBE, on_subscribe)
        r = await s.publish_namespace(namespace=a.ns, wait_response=True)
        say(f"PUBLISH_NAMESPACE reply {type(r).__name__}")
        while not os.path.exists(a.gate):
            await asyncio.sleep(0.1)
        say(f"GATE subs={sorted(subs)}")
        prof = s._profile
        for g in range(a.groups):
            for name, ids, ext_on in (("g", [0, 1, 3, 7], {1, 3}), ("e", [0, 1, 2, 3], {0, 1, 2, 3})):
                if name not in subs:
                    continue
                alias = subs[name][0]
                sid = await s.open_uni_stream()
                hdr = SubgroupHeader(track_alias=alias, group_id=g, subgroup_id=0, publisher_priority=128,
                                     extensions_present=True, prof=prof)
                s.stream_write(sid, hdr.serialize().data)
                for o in ids:
                    buf = hdr.next_object(payload=payload(name, g, o), extensions=EXT if o in ext_on else None,
                                          object_id=o)
                    await s.stream_write_drain(sid, buf.data)
                s.stream_write(sid, b"", end_stream=True)
            if "d" in subs:
                alias = subs["d"][0]
                for o in range(5):
                    dg = ObjectDatagram(track_alias=alias, group_id=g, object_id=o, publisher_priority=128,
                                        extensions=EXT if o == 1 else None, payload=payload("d", g, o),
                                        end_of_group=(o == 4))
                    await s.dgram_write_drain(dg.serialize(prof=prof))
            await asyncio.sleep(0.2)
        say(f"SENT {a.groups} groups")
        await asyncio.sleep(3)
        if a.end == "clean":
            from aiomoqt.messages import SubscribeDone
            for name, (alias, rid) in subs.items():
                try:
                    s._send_reply(rid, SubscribeDone(request_id=rid, status_code=2,
                                                     stream_count=a.groups if name != "d" else 0,
                                                     reason="track ended"), fin=True)
                except Exception as e:        # noqa: BLE001
                    say(f"publish_done {name}: {e!r}")
            s.publish_namespace_done(namespace=s._make_namespace_tuple(a.ns), request_id=getattr(r, "request_id", None))
            say("END clean: PUBLISH_DONE x%d + PUBLISH_NAMESPACE_DONE" % len(subs))
            await asyncio.sleep(2)
        elif a.end == "nsdone":
            s.publish_namespace_done(namespace=s._make_namespace_tuple(a.ns), request_id=getattr(r, "request_id", None))
            say("END nsdone: PUBLISH_NAMESPACE_DONE only")
            await asyncio.sleep(5)
        elif a.end == "hang":
            say("END hang: waiting to be killed")
            await asyncio.sleep(120)
        else:
            say("END abrupt: close")
            s._quic.close() if hasattr(s, "_quic") else None
            return
        say("EXIT")


async def run_sub(a):
    from aiomoqt.track import SubscribedTrack
    rec = []

    def cb_for(n):
        def cb(msg, consumed, now, group_id, subgroup_id=None):
            st = getattr(msg, "status", ObjectStatus.NORMAL)
            ext = getattr(msg, "extensions", None)
            eog = getattr(msg, "end_of_group", None)
            p = bytes(getattr(msg, "payload", b"") or b"")
            ok = p == payload(n, group_id, msg.object_id) if st == ObjectStatus.NORMAL else None
            rec.append((n, group_id, msg.object_id))
            say(f"OBJ {n} g={group_id} o={msg.object_id} sg={subgroup_id} kind={type(msg).__name__} "
                f"status={getattr(st, 'name', st)} eog_bit={eog} ext={ext} len={len(p)} payload_ok={ok}")
        return cb

    async with client(a).connect() as s:
        await s.client_session_init()
        for n in ("d", "g", "e"):
            t = SubscribedTrack(s, namespace=a.ns, trackname=n, on_object=cb_for(n),
                                on_done=lambda m, n=n: say(f"PUBLISH_DONE {n}: {m}"))
            try:
                await t.subscribe(timeout=10)
                say(f"SUBSCRIBED {n} alias={t.track_alias}")
            except Exception as e:            # noqa: BLE001
                say(f"SUBSCRIBE {n} failed: {e!r}")
        t0 = time.time()
        while time.time() - t0 < a.duration:
            await asyncio.sleep(0.5)
            if getattr(s, "_closed", None) and s._closed.is_set():
                say("SESSION CLOSED by peer")
                break
        say(f"SUB-DONE {len(rec)} objects")


async def run_dup(a):
    async with client(a).connect() as s:
        await s.client_session_init()
        try:
            r = await asyncio.wait_for(s.publish_namespace(namespace=a.ns, wait_response=True), 8)
            say(f"DUP PUBLISH_NAMESPACE reply {type(r).__name__} {r}")
        except Exception as e:                # noqa: BLE001
            say(f"DUP PUBLISH_NAMESPACE: {type(e).__name__} {e}")
        await asyncio.sleep(2)


async def run_push(a):
    from aiomoqt.track import PublishedTrack
    async with client(a).connect() as s:
        await s.client_session_init()
        try:
            r = await asyncio.wait_for(s.publish(namespace=a.ns, track_name="p", wait_response=True), 8)
            say(f"PUBLISH reply {type(r).__name__} {r}")
        except Exception as e:                # noqa: BLE001
            say(f"PUBLISH: {type(e).__name__} {e}")
        await asyncio.sleep(2)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("role", choices=["pub", "sub", "dup", "push"])
    p.add_argument("relay")
    p.add_argument("ns")
    p.add_argument("arg", nargs="?")
    p.add_argument("--draft", type=int, default=16)
    p.add_argument("--end", choices=["clean", "abrupt", "nsdone", "hang"], default="clean")
    p.add_argument("--groups", type=int, default=10)
    a = p.parse_args()
    if a.role == "pub":
        a.gate = a.arg
        asyncio.run(run_pub(a))
    elif a.role == "sub":
        a.duration = float(a.arg)
        asyncio.run(run_sub(a))
    else:
        asyncio.run({"dup": run_dup, "push": run_push}[a.role](a))


if __name__ == "__main__":
    main()
