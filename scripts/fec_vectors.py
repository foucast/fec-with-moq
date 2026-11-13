"""Test vectors for the TypeScript decoder (web/fec-decoder), from our Python encoder.

    FECMOQ_CODER=isal .venv/bin/python scripts/fec_vectors.py > web/fec-decoder/test/vectors.json

Needs ISA-L (libisal), so run it where the runs run (a subscriber VM). Every block's repair
symbols come from codec.Coder(k, m, "isal") and are packed with wire.pack_repair exactly as the
publisher sends them. Each case names what was lost (source objects, or source symbols directly)
and which repair symbols arrived; "expect" says whether it must decode, and the generator checks
that against our own Python decoder before writing it.

Blocks: small synthetic ones (k = 5, 20), and arm-B blocks as the runs send them (make_block,
seed 7, 12 objects, rungs 0.15 x 3). Wire samples: plain and INDEXED repair objects.
"""
import base64, json, random, sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
from fecmoq.codec import Coder                                     # noqa: E402
from fecmoq.moq import make_block, rung_slices                     # noqa: E402
from fecmoq.symbols import SPAN, SYMBOL_SIZE, erased_symbols, layout_block, to_symbols  # noqa: E402
from fecmoq.wire import pack_repair, repair_index, unpack_repair   # noqa: E402

b64 = lambda b: base64.b64encode(bytes(b)).decode()               # noqa: E731
rng = random.Random(20261010)


def check(coder, k, symbols, repairs, missing, rep_avail):
    avail = {i: symbols[i] for i in range(k) if i not in missing}
    avail.update({k + j: repairs[j] for j in rep_avail})
    out = coder.decode(avail)
    return out is not None and out == symbols


def block(name, objects, m, cases_spec):
    sizes = [len(o) for o in objects]
    lay = layout_block(sizes, SPAN)
    k = lay.k
    symbols = to_symbols(lay, objects)
    coder = Coder(k, m, "isal")
    repairs = coder.encode(symbols)
    cases = []
    for spec in cases_spec:
        lost_obj = spec.get("lost_objects")
        missing = sorted(erased_symbols(lay, lost_obj)) if lost_obj is not None else sorted(spec["missing_symbols"])
        rep_avail = sorted(spec["repair_available"])
        ok = check(coder, k, symbols, repairs, set(missing), rep_avail)
        expect = "ok" if len(missing) <= len(rep_avail) else "fail"
        assert ok == (expect == "ok"), (name, spec["name"])        # MDS: decodable iff e <= repairs
        c = {"name": spec["name"], "missing_symbols": missing, "repair_available": rep_avail, "expect": expect}
        if lost_obj is not None:
            c["lost_objects"] = sorted(lost_obj)
        cases.append(c)
    return {"name": name, "k": k, "m": m, "symbol_size": SYMBOL_SIZE, "sizes": sizes,
            "objects": [b64(o) for o in objects],
            "repair_objects": [b64(pack_repair(sizes, repairs[j], "isal")) for j in range(m)],
            "cases": cases}


def random_cases(k, m, n_objects, extra=()):
    out = [{"name": "no loss", "missing_symbols": [], "repair_available": []}]
    for t in range(6):
        e = rng.randint(1, m)
        miss = rng.sample(range(k), e)
        reps = rng.sample(range(m), rng.randint(e, m))
        out.append({"name": f"random e={e} r={len(reps)}", "missing_symbols": miss, "repair_available": reps})
    out.append({"name": f"max erasure e=m={m}", "missing_symbols": rng.sample(range(k), m),
                "repair_available": list(range(m))})
    out.append({"name": f"max erasure, last rows only", "missing_symbols": list(range(k - m, k)) if m <= k else [],
                "repair_available": list(range(m))})
    if m + 1 <= k:
        out.append({"name": f"too many e=m+1={m + 1}", "missing_symbols": rng.sample(range(k), m + 1),
                    "repair_available": list(range(m))})
    e = min(m, k)
    out.append({"name": f"too few repairs e={e} r={e - 1}", "missing_symbols": rng.sample(range(k), e),
                "repair_available": rng.sample(range(m), e - 1)})
    out.append({"name": "all source lost, k repairs" if m >= k else "first object lost, all repairs",
                "lost_objects": [0], "repair_available": list(range(m))})
    out.extend(extra)
    return out


def synth_objects(n, total):
    cuts = sorted(rng.sample(range(1, total), n - 1))
    sizes = [b - a for a, b in zip([0] + cuts, cuts + [total])]
    return [bytes(rng.getrandbits(8) for _ in range(s)) for s in sizes]


blocks = []
objs = synth_objects(3, 5 * SYMBOL_SIZE - 37)                        # k = 5
blocks.append(block("synthetic k=5 m=3", objs, 3, random_cases(5, 3, 3)))
objs = synth_objects(6, 20 * SYMBOL_SIZE - 500)                      # k = 20
blocks.append(block("synthetic k=20 m=7", objs, 7, random_cases(20, 7, 6)))
for g in (0, 1):
    objs = make_block(7, g, 12)
    lay = layout_block([len(o) for o in objs], SPAN)
    sl = rung_slices(lay.k, [0.15] * 3)
    m = sl[-1].stop
    extra = [
        {"name": "lost objects 3,7; rungs 1-3", "lost_objects": [3, 7], "repair_available": list(range(m))},
        {"name": "lost object 0 (I-frame); rung 1 only", "lost_objects": [0], "repair_available": list(sl[0])},
        {"name": "lost objects 1,2; rung 1 only", "lost_objects": [1, 2], "repair_available": list(sl[0])},
    ]
    blocks.append(block(f"arm-B block {g} (make_block seed 7, 12 objects, rungs 0.15x3)", objs, m,
                        random_cases(lay.k, m, 12, extra)))

# wire samples: a plain and an INDEXED repair object, with the fields a parser must return
b = blocks[2]
sizes = b["sizes"]
sym = base64.b64decode(b["repair_objects"][5])[2 + 2 * len(sizes):]
wire = []
for idx in (None, 5):
    p = pack_repair(sizes, sym, "isal", index=idx)
    us, usym = unpack_repair(p, expect="isal")
    wire.append({"name": "indexed" if idx is not None else "plain", "object": b64(p), "codec": 2,
                 "indexed": idx is not None, "n_objects": len(sizes), "sizes": us,
                 "index": repair_index(p), "symbol": b64(usym), "length": len(p)})

json.dump({"generator": "scripts/fec_vectors.py", "codec": "isal", "field": "GF(2^8) poly 0x11d",
           "matrix": "systematic Cauchy, row i>=k col c = gf_inv(i XOR c) (ISA-L gf_gen_cauchy1_matrix)",
           "symbol_size": SYMBOL_SIZE, "blocks": blocks, "wire": wire}, sys.stdout, separators=(",", ":"))
