"""Media-block vectors for web/fec-decoder (Shaka phase 2, stage B): real LOC frames from a GOP-12 mp4
as the FEC publisher builds them (MediaBlockSchedule), with their source and repair objects as sent.

    FECMOQ_CODER=isal python scripts/fec_media_vectors.py CLIP.mp4 > web/fec-decoder/test/media-vectors.json
    FECMOQ_CODER=isal python scripts/fec_media_vectors.py CLIP-G120.mp4 120 \
        > web/fec-decoder/test/media-vectors-gop120.json   # 12-frame blocks of a 120-frame GOP
"""
import base64, json, os, random, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
from fecmoq.codec import Coder                                     # noqa: E402
from fecmoq.media import MediaBlockSchedule, unpack_loc_object     # noqa: E402
from fecmoq.moq import rung_slices                                 # noqa: E402
from fecmoq.symbols import SPAN, erased_symbols, layout_block, to_symbols  # noqa: E402
from fecmoq.wire import pack_repair                                # noqa: E402

b64 = lambda b: base64.b64encode(bytes(b)).decode()               # noqa: E731
gop = int(sys.argv[2]) if len(sys.argv) > 2 else None
sch = MediaBlockSchedule(media=sys.argv[1], gop_frames=gop, seed=7, n_blocks=6, n_objects=12, gap_s=0.2)
rng = random.Random(11)
blocks = []
for b in range(6):
    objs = sch.block(b)
    sizes = [len(o) for o in objs]
    lay = layout_block(sizes, SPAN)
    syms = to_symbols(lay, objs)
    m = rung_slices(lay.k, [0.15] * 3)[-1].stop
    rep = Coder(lay.k, m, "isal").encode(syms)
    cases = [{"name": "keyframe lost, all rungs", "lost_objects": [0], "repair_available": list(range(m))},
             {"name": "two frames lost, all rungs", "lost_objects": sorted(rng.sample(range(1, 12), 2)),
              "repair_available": list(range(m))},
             {"name": "max symbol erasure", "missing_symbols": sorted(rng.sample(range(lay.k), m)),
              "repair_available": list(range(m))},
             {"name": "too many erasures", "missing_symbols": sorted(rng.sample(range(lay.k), m + 1)),
              "repair_available": list(range(m))}]
    for c in cases:
        miss = erased_symbols(lay, c["lost_objects"]) if "lost_objects" in c else c["missing_symbols"]
        c["expect"] = "ok" if len(miss) <= len(c["repair_available"]) else "fail"
    blocks.append({"block": b, "k": lay.k, "m": m, "sizes": sizes,
                   "objects": [b64(o) for o in objs],
                   "frames": [b64(unpack_loc_object(o)[1]) for o in objs],
                   "source_objects": [b64(pack_repair(sizes, s, "isal")) for s in syms],
                   "repair_objects": [b64(pack_repair(sizes, r, "isal")) for r in rep],
                   "cases": cases})
json.dump({"generator": "scripts/fec_media_vectors.py", "clip": os.path.basename(sys.argv[1]),
           "codec": sch.media.codec, "blocks": blocks}, sys.stdout, separators=(",", ":"))
