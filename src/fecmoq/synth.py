"""Synthetic source objects with a realistic size distribution.

No encoder: the repair layer treats objects as opaque bytes, so a plausible size
distribution is all that matters. One large I-frame-sized object leads each
block, followed by smaller P-frame-sized objects, inside the 5-50KB range the
design note assumes. Seeded, so every result is reproducible.
"""
from __future__ import annotations

import random

I_MEAN, I_SD = 40_000, 5_000
P_LO, P_HI = 5_000, 12_000


def object_sizes(n_objects: int, rng: random.Random) -> list[int]:
    i = max(5_000, min(50_000, int(rng.gauss(I_MEAN, I_SD))))
    return [i] + [rng.randint(P_LO, P_HI) for _ in range(n_objects - 1)]


def objects(sizes, rng: random.Random) -> list[bytes]:
    return [rng.randbytes(s) for s in sizes]
