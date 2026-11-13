"""Milestone 2a correctness: lose whole source objects, reconstruct from repair."""
import random

import pytest

from fecmoq.codec import Coder
from fecmoq.symbols import PAD, SPAN, erased_symbols, from_symbols, layout_block, to_symbols
from fecmoq.synth import object_sizes, objects


@pytest.mark.parametrize("mode", [SPAN, PAD])
@pytest.mark.parametrize("seed", range(25))
def test_lost_objects_reconstruct_byte_identical(mode, seed):
    rng = random.Random(seed)
    objs = objects(object_sizes(12, rng), rng)
    lay = layout_block([len(o) for o in objs], mode)
    src = to_symbols(lay, objs)

    lost = set(rng.sample(range(len(objs)), 2))
    erased = erased_symbols(lay, lost)
    coder = Coder(lay.k, len(erased))                 # exactly enough repair
    repair = coder.encode(src)

    avail = {i: s for i, s in enumerate(src) if i not in erased}
    avail.update({lay.k + j: r for j, r in enumerate(repair)})
    got = from_symbols(lay, coder.decode(avail))
    assert got == objs


def test_one_short_of_enough_repair_fails():
    rng = random.Random(1)
    objs = objects(object_sizes(12, rng), rng)
    lay = layout_block([len(o) for o in objs], SPAN)
    src = to_symbols(lay, objs)
    erased = erased_symbols(lay, {3})
    coder = Coder(lay.k, len(erased) - 1)
    avail = {i: s for i, s in enumerate(src) if i not in erased}
    avail.update({lay.k + j: r for j, r in enumerate(coder.encode(src))})
    assert coder.decode(avail) is None


def test_zero_repair_is_plain_subscriber():
    """Systematic: taking no repair track leaves an ordinary subscriber."""
    rng = random.Random(2)
    objs = objects(object_sizes(12, rng), rng)
    lay = layout_block([len(o) for o in objs], SPAN)
    assert from_symbols(lay, to_symbols(lay, objs)) == objs
