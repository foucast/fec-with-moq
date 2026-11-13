"""Encode/decode time per block for each coder backend. No network.

    python scripts/bench_coders.py [--k 131] [--repair 0.45] [--runs 200]
"""
import argparse
import os
import random
import statistics as st
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from fecmoq.codec import Coder, IsalIncrementalEncoder  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--k", type=int, default=131)
p.add_argument("--repair", type=float, default=0.45)
p.add_argument("--sym", type=int, default=1100)
p.add_argument("--runs", type=int, default=200)
a = p.parse_args()
k, m = a.k, round(a.k * a.repair)
rng = random.Random(1)
src = [rng.randbytes(a.sym) for _ in range(k)]
print(f"k={k} m={m} symbol={a.sym}B block={k*a.sym/1024:.0f}KB, {a.runs} runs, p50 / p95 ms")


def t(fn):
    xs = []
    for _ in range(a.runs):
        s = time.perf_counter(); fn(); xs.append((time.perf_counter() - s) * 1e3)
    xs.sort()
    return f"{st.median(xs):7.3f} / {xs[int(.95 * len(xs))]:7.3f}"


try:
    import zfec  # noqa: F401  -- optional comparison backend (GPL-2+), not installed by default
    backends = ("zfec", "isal")
except ImportError:
    backends = ("isal",)

for b in backends:
    rep = Coder(k, m, b).encode(src)
    avail = {i: s for i, s in enumerate(src) if i >= m}          # lose m source symbols
    avail.update({k + j: r for j, r in enumerate(rep)})          # worst case: all repair used
    print(f"  {b:5} encode {t(lambda: Coder(k, m, b).encode(src))}   "
          f"decode (m lost) {t(lambda: Coder(k, 0, b).decode(avail))}")


def inc_last_step():
    e = IsalIncrementalEncoder(k, m, a.sym)
    for i in range(k - 1):
        e.add(i, src[i])
    s = time.perf_counter(); e.add(k - 1, src[k - 1]); e.repair()
    return (time.perf_counter() - s) * 1e3


xs = sorted(inc_last_step() for _ in range(max(20, a.runs // 10)))
print(f"  isal incremental: time after the LAST source symbol arrives  p50 {st.median(xs):.3f} ms")
