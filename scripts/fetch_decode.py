"""Independent decoder for MoQT draft-16 FETCH streams, to locate FETCH payload corruption.

    python scripts/fetch_decode.py CAPTURE.pkl [WIRE.json]

CAPTURE.pkl comes from `fetch_check.py --capture DIR`: each incoming stream's bytes as the
QUIC layer (aiopquic) handed them to aiomoqt, aiomoqt's decoded objects, and the expected
payloads from the seed. WIRE.json (optional) maps stream id -> hex bytes reassembled from a
decrypted packet capture (tshark), i.e. what the relay put on the wire.

The parser follows moxygen's writer (MoQFramer.cpp writeFetchHeader / writeFetchObjectDraft15
/ writeStreamObject, openmoq/moxygen 6102eac), not aiomoqt's reader, so it is an independent
check: FETCH_HEADER = type 0x05, request id; then per object a serialization-flags varint
(0x03 subgroup mode, 0x04 object id present, 0x08 group id present, 0x10 priority present,
0x20 extensions present, 0x40 datagram), the present fields (absent ones inherit: group = prior,
object id = prior + 1, priority = prior; subgroup per mode), extensions (length-prefixed) if
flagged, payload length, then the payload (or, for length 0, a status varint).

For every fetched (track, group) it reports, per view, how many objects decode and how many
match the seed, and where the views first differ.
"""
import json
import pickle
import sys


def varint(b, i):
    first = b[i]
    n = 1 << (first >> 6)
    v = first & 0x3F
    for k in range(1, n):
        v = (v << 8) | b[i + k]
    return v, i + n


def parse_fetch(b):
    """-> (request_id, [(group, subgroup, object, priority, payload_offset, payload)], note)."""
    objs, note = [], ""
    try:
        t, i = varint(b, 0)
        if t != 0x05:
            return None, [], f"not a FETCH stream (type 0x{t:x})"
        rid, i = varint(b, i)
        g = sg = o = pr = None
        while i < len(b):
            start = i
            flags, i = varint(b, i)
            if flags & 0x08:
                g, i = varint(b, i)
            if flags & 0x40:
                sg = 0
            else:
                mode = flags & 0x03
                if mode == 0:
                    sg = 0
                elif mode == 1:
                    pass
                elif mode == 2:
                    sg = (sg or 0) + 1
                else:
                    sg, i = varint(b, i)
            if flags & 0x04:
                o, i = varint(b, i)
            else:
                o = 0 if o is None else o + 1
            if flags & 0x10:
                pr = b[i]
                i += 1
            if flags & 0x20:
                ln, i = varint(b, i)
                i += ln
            plen, i = varint(b, i)
            if plen == 0:
                _status, i = varint(b, i)
                objs.append((g, sg, o, pr, i, b""))
                continue
            if i + plen > len(b):
                note = f"stream ends inside object {o} ({len(b) - i} of {plen} bytes)"
                objs.append((g, sg, o, pr, i, bytes(b[i:])))
                break
            objs.append((g, sg, o, pr, i, bytes(b[i:i + plen])))
            i += plen
    except IndexError:
        note = f"stream ends inside an object header at byte {start}"
    return rid, objs, note


def first_diff(x, y):
    return next((k for k, (p, q) in enumerate(zip(x, y)) if p != q),
                None if len(x) == len(y) else min(len(x), len(y)))


def main():
    cap = pickle.load(open(sys.argv[1], "rb"))
    wire = {}
    if len(sys.argv) > 2:
        wire = {int(k): bytes.fromhex(v) for k, v in json.load(open(sys.argv[2])).items()}
    # request id -> stream id, from each stream's FETCH_HEADER
    by_rid = {}
    for sid, b in cap["raw"].items():
        rid, objs, note = parse_fetch(b)
        if rid is not None:
            by_rid[rid] = (sid, objs, note)
    for track, g, start, rid in cap["fetches"]:
        want = cap["expected"][(track, g)]
        dec = cap["decoded"].get(rid, {})
        line = f"{track} g{g} from {start} (req {rid}):"
        if rid not in by_rid:
            print(line, "no FETCH stream captured")
            continue
        sid, objs, note = by_rid[rid]
        raw_ok = sum(1 for _g, _s, o, _p, _off, pl in objs if want.get(o) == pl)
        dec_ok = sum(1 for o, (pl, gg) in dec.items() if want.get(o) == pl)
        same_dec = sum(1 for _g, _s, o, _p, _off, pl in objs if o in dec and dec[o][0] == pl)
        out = [f"stream {sid}", f"raw-parse {len(objs)} objs, {raw_ok} match seed",
               f"aiomoqt {len(dec)} objs, {dec_ok} match seed",
               f"raw-parse == aiomoqt on {same_dec}/{len(objs)}"]
        if sid in wire:
            w = wire[sid]
            r = cap["raw"][sid]
            d = first_diff(w, r)
            _wr, wobjs, wnote = parse_fetch(w)
            wire_ok = sum(1 for _g, _s, o, _p, _off, pl in wobjs if want.get(o) == pl)
            out.append(f"wire {len(w)}B vs raw {len(r)}B: " + ("identical" if d is None else f"first diff @{d}"))
            out.append(f"wire-parse {len(wobjs)} objs, {wire_ok} match seed" + (f" ({wnote})" if wnote else ""))
        if note:
            out.append(note)
        print(line, "; ".join(out))
        for _g, _s, o, _p, off, pl in objs:
            w_ = want.get(o)
            if w_ is not None and w_ != pl:
                k = first_diff(pl, w_)
                print(f"    obj {o}: payload @stream byte {off}, {len(pl)}B (want {len(w_)}B); first diff at "
                      f"payload byte {k} = stream byte {off + (k or 0)}")


if __name__ == "__main__":
    main()
