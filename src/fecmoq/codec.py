"""Erasure-coder interface, with two selectable backends.

Everything else talks to `Coder`, never to a library directly, so backends can be
swapped or compared side by side.

  isal  -- Intel ISA-L Reed-Solomon (Cauchy matrix), SIMD-accelerated, BSD-3.
           The default. Needs the system library: `apt install libisal-dev` (or
           libisal2). Also offers incremental encoding (IsalIncrementalEncoder).
  zfec  -- table-lookup Reed-Solomon (Vandermonde-based), the original prototype
           coder: no SIMD, slow at large m. Optional and NOT installed by default:
           zfec is GPL-2+. `pip install zfec` yourself if you want to compare.

Selection: `Coder(k, m, backend=...)`, else the FECMOQ_CODER environment variable,
else "isal".

⚠️ The two backends produce DIFFERENT repair bytes for the same block. Publisher
and subscriber must use the same backend. The repair wire header records which
one encoded each object (fecmoq.wire), and unpack_repair refuses a mismatch rather
than letting a receiver decode garbage.

Both work over GF(2^8): k + m <= 256. With SYMBOL_SIZE=1100 that caps a block,
source plus repair, near 280KB.

Interchangeability, required by the ladder, holds for both: repair symbol j is
byte-identical whatever total m the publisher generated, so a subscriber on any
subset of rungs decodes without knowing how many rungs exist. For zfec this was
verified on 2026-09-27; for ISA-L it follows from the Cauchy construction (row j
depends only on j and k) and is covered by tests/test_codec_backends.py.
"""
from __future__ import annotations

import ctypes
import ctypes.util
import functools
import os

MAX_N = 256
BACKENDS = ("zfec", "isal")


def default_backend() -> str:
    b = os.environ.get("FECMOQ_CODER", "isal").strip().lower()
    if b not in BACKENDS:
        raise ValueError(f"FECMOQ_CODER={b!r}; expected one of {BACKENDS}")
    return b


def _check(k: int, m: int) -> None:
    if k < 1 or m < 0 or k + m > MAX_N:
        raise ValueError(f"need 1 <= k and k + m <= {MAX_N}; got k={k}, m={m}")


# --------------------------------------------------------------------------- zfec
def _zfec():
    try:
        import zfec
    except ImportError as e:
        raise RuntimeError("the zfec backend is optional and not installed (pip install zfec; GPL-2+)") from e
    return zfec


class ZfecCoder:
    name = "zfec"

    def __init__(self, k: int, m: int):
        _check(k, m)
        self.k, self.m = k, m

    def encode(self, source: list[bytes]) -> list[bytes]:
        """Return the m repair symbols. Systematic: source symbols are sent as-is."""
        if len(source) != self.k:
            raise ValueError("need exactly k source symbols")
        if self.m == 0:
            return []
        zfec = _zfec()
        shares = zfec.Encoder(self.k, self.k + self.m).encode(source)
        return [bytes(s) for s in shares[self.k:]]

    def decode(self, available: dict[int, bytes]) -> list[bytes] | None:
        """available: symbol index (0..k+m-1) -> bytes. Returns k source symbols, or None."""
        if len(available) < self.k:
            return None
        idx = sorted(available)[: self.k]
        out = _zfec().Decoder(self.k, MAX_N).decode([available[i] for i in idx], idx)
        return [bytes(b) for b in out]


# --------------------------------------------------------------------------- ISA-L
@functools.lru_cache(maxsize=1)
def _isal():
    path = ctypes.util.find_library("isal") or "libisal.so.2"
    try:
        lib = ctypes.CDLL(path)
    except OSError as e:
        raise RuntimeError("ISA-L not found: install libisal-dev (or libisal2)") from e
    u8p = ctypes.POINTER(ctypes.c_ubyte)
    lib.gf_gen_cauchy1_matrix.argtypes = [u8p, ctypes.c_int, ctypes.c_int]
    lib.ec_init_tables.argtypes = [ctypes.c_int, ctypes.c_int, u8p, u8p]
    lib.ec_encode_data.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_int, u8p,
                                   ctypes.POINTER(u8p), ctypes.POINTER(u8p)]
    lib.ec_encode_data_update.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int,
                                          u8p, u8p, ctypes.POINTER(u8p)]
    lib.gf_invert_matrix.argtypes = [u8p, u8p, ctypes.c_int]
    lib.gf_invert_matrix.restype = ctypes.c_int
    return lib


@functools.lru_cache(maxsize=64)
def _cauchy(rows: int, k: int):
    """Systematic Cauchy matrix, rows x k. Row i (i >= k) depends only on i and k."""
    a = (ctypes.c_ubyte * (rows * k))()
    _isal().gf_gen_cauchy1_matrix(a, rows, k)
    return bytes(a)


@functools.lru_cache(maxsize=64)
def _encode_tables(k: int, m: int):
    mat = _cauchy(k + m, k)
    sub = (ctypes.c_ubyte * (m * k)).from_buffer_copy(mat[k * k:])
    tbl = (ctypes.c_ubyte * (32 * k * m))()
    _isal().ec_init_tables(k, m, sub, tbl)
    return tbl


def _ptrs(bufs):
    u8p = ctypes.POINTER(ctypes.c_ubyte)
    arr = (u8p * len(bufs))()
    for i, b in enumerate(bufs):
        arr[i] = ctypes.cast(b, u8p)
    return arr


def _inbufs(symbols):
    # ISA-L reads these; copy into ctypes buffers so pointers stay valid.
    return [ctypes.create_string_buffer(s, len(s)) for s in symbols]


class IsalCoder:
    name = "isal"

    def __init__(self, k: int, m: int):
        _check(k, m)
        self.k, self.m = k, m

    def encode(self, source: list[bytes]) -> list[bytes]:
        if len(source) != self.k:
            raise ValueError("need exactly k source symbols")
        if self.m == 0:
            return []
        n = len(source[0])
        if any(len(s) != n for s in source):
            raise ValueError("source symbols must be equal length")
        src = _inbufs(source)
        out = [ctypes.create_string_buffer(n) for _ in range(self.m)]
        _isal().ec_encode_data(n, self.k, self.m, _encode_tables(self.k, self.m),
                               _ptrs(src), _ptrs(out))
        return [o.raw for o in out]

    def decode(self, available: dict[int, bytes]) -> list[bytes] | None:
        """Solve only for the erased source symbols.

        The code is systematic, so with e source symbols missing the problem is e x e,
        not k x k: invert the e x e block of the Cauchy rows for e repair symbols,
        restricted to the missing columns. Done in two ISA-L passes so no GF arithmetic
        runs in Python: (1) syndrome = repair + C[R, known] * known source;
        (2) missing source = inverse * syndrome.
        """
        k = self.k
        if len(available) < k:
            return None
        missing = [i for i in range(k) if i not in available]
        e = len(missing)
        if e == 0:
            return [bytes(available[i]) for i in range(k)]
        repairs = sorted(i for i in available if i >= k)[:e]
        if len(repairs) < e:
            return None
        known = [j for j in range(k) if j in available]
        lib = _isal()
        full = _cauchy(max(repairs) + 1, k)
        rowsC = [full[r * k:(r + 1) * k] for r in repairs]
        # e x e block on the missing columns, inverted
        sq = (ctypes.c_ubyte * (e * e))(*[row[c] for row in rowsC for c in missing])
        inv = (ctypes.c_ubyte * (e * e))()
        if lib.gf_invert_matrix(sq, inv, e) != 0:
            return None
        n = len(available[repairs[0]])
        # pass 1: syndrome over inputs [known source..., repair...] with [C[R,known] | I]
        kin = len(known) + e
        m1 = bytearray(e * kin)
        for r, row in enumerate(rowsC):
            base = r * kin
            for c, j in enumerate(known):
                m1[base + c] = row[j]
            m1[base + len(known) + r] = 1
        t1 = (ctypes.c_ubyte * (32 * kin * e))()
        lib.ec_init_tables(kin, e, (ctypes.c_ubyte * len(m1)).from_buffer(m1), t1)
        ins = _inbufs([available[j] for j in known] + [available[r] for r in repairs])
        syn = [ctypes.create_string_buffer(n) for _ in range(e)]
        lib.ec_encode_data(n, kin, e, t1, _ptrs(ins), _ptrs(syn))
        # pass 2: missing = inverse * syndrome
        t2 = (ctypes.c_ubyte * (32 * e * e))()
        lib.ec_init_tables(e, e, inv, t2)
        out = [ctypes.create_string_buffer(n) for _ in range(e)]
        lib.ec_encode_data(n, e, e, t2, _ptrs(syn), _ptrs(out))
        rec = dict(zip(missing, (o.raw for o in out)))
        return [bytes(available[i]) if i in available else rec[i] for i in range(k)]


class IsalIncrementalEncoder:
    """Update parity as each source symbol is produced, instead of after the block.

    Feed source symbols in any order with add(i, symbol); repair() returns the m repair
    symbols, identical to IsalCoder(k, m).encode(...) once all k have been added. This
    is how to take most of the encode off the critical path: parity is nearly done when
    the block's last source object exists.
    """

    def __init__(self, k: int, m: int, symbol_size: int):
        _check(k, m)
        self.k, self.m, self.n = k, m, symbol_size
        self._out = [ctypes.create_string_buffer(symbol_size) for _ in range(m)]
        self._optr = _ptrs(self._out)
        self._seen: set[int] = set()

    def add(self, i: int, symbol: bytes) -> None:
        if not 0 <= i < self.k or i in self._seen:
            raise ValueError(f"source index {i} out of range or already added")
        if len(symbol) != self.n:
            raise ValueError("symbol length mismatch")
        buf = ctypes.create_string_buffer(symbol, self.n)
        _isal().ec_encode_data_update(self.n, self.k, self.m, i, _encode_tables(self.k, self.m),
                                      ctypes.cast(buf, ctypes.POINTER(ctypes.c_ubyte)), self._optr)
        self._seen.add(i)

    def repair(self) -> list[bytes]:
        if len(self._seen) != self.k:
            raise ValueError(f"only {len(self._seen)} of {self.k} source symbols added")
        return [o.raw for o in self._out]


# --------------------------------------------------------------------------- facade
_IMPL = {"zfec": ZfecCoder, "isal": IsalCoder}


def Coder(k: int, m: int, backend: str | None = None):
    """Factory: Coder(k, m) uses FECMOQ_CODER, else ISA-L."""
    b = (backend or default_backend()).lower()
    if b not in _IMPL:
        raise ValueError(f"unknown coder backend {b!r}; expected one of {BACKENDS}")
    return _IMPL[b](k, m)
