"""Both coder backends must satisfy the properties the protection ladder relies on."""
import os
import random

import pytest

from fecmoq.codec import Coder, IsalCoder, IsalIncrementalEncoder
from fecmoq.wire import pack_repair, repair_codec, unpack_repair

try:
    import zfec  # noqa: F401  -- optional backend (GPL-2+), not installed by default
    HAVE_ZFEC = True
except ImportError:
    HAVE_ZFEC = False
needs_zfec = pytest.mark.skipif(not HAVE_ZFEC, reason="zfec not installed (optional backend)")
BACKENDS = [pytest.param("zfec", marks=needs_zfec), "isal"]
SYM = 1100


def _block(seed, k):
    rng = random.Random(seed)
    return [rng.randbytes(SYM) for _ in range(k)]


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("seed", range(15))
def test_any_k_of_n_decode(backend, seed):
    rng = random.Random(seed)
    k = rng.randint(10, 140)
    m = rng.randint(1, min(100, 256 - k))
    src = _block(seed, k)
    rep = Coder(k, m, backend).encode(src)
    allsyms = {i: s for i, s in enumerate(src)}
    allsyms.update({k + j: r for j, r in enumerate(rep)})
    keep = rng.sample(sorted(allsyms), k)              # any k of n, source or repair
    out = Coder(k, 0, backend).decode({i: allsyms[i] for i in keep})
    assert out == src


@pytest.mark.parametrize("backend", BACKENDS)
def test_repair_symbol_independent_of_total(backend):
    """The ladder's interchangeability: symbol j is the same whatever m the publisher chose."""
    k = 60
    src = _block(1, k)
    small = Coder(k, 10, backend).encode(src)
    large = Coder(k, 90, backend).encode(src)
    assert small == large[:10]


@pytest.mark.parametrize("backend", BACKENDS)
def test_one_short_fails(backend):
    k, m = 40, 12
    src = _block(2, k)
    rep = Coder(k, m, backend).encode(src)
    avail = {i: s for i, s in enumerate(src) if i >= 13}           # lose 13 source
    avail.update({k + j: r for j, r in enumerate(rep)})            # only 12 repair
    assert Coder(k, 0, backend).decode(avail) is None


@needs_zfec
def test_backends_differ_so_wire_must_say_which():
    k = 20
    src = _block(3, k)
    assert Coder(k, 5, "zfec").encode(src) != Coder(k, 5, "isal").encode(src)


def test_isal_incremental_matches_batch():
    k, m = 120, 54
    src = _block(4, k)
    inc = IsalIncrementalEncoder(k, m, SYM)
    for i in random.Random(5).sample(range(k), k):                 # any arrival order
        inc.add(i, src[i])
    assert inc.repair() == IsalCoder(k, m).encode(src)


def test_wire_records_codec_and_rejects_mismatch():
    p = pack_repair([1000, 2000], b"x" * SYM, codec="isal")
    assert repair_codec(p) == "isal"
    assert unpack_repair(p, expect="isal")[0] == [1000, 2000]
    with pytest.raises(ValueError, match="FECMOQ_CODER"):
        unpack_repair(p, expect="zfec")


def test_zfec_wire_unchanged():
    """zfec objects keep codec byte 1 == the original VERSION byte."""
    assert pack_repair([1000], b"y" * SYM, codec="zfec")[0] == 1
