// GF(2^8) with the polynomial x^8 + x^4 + x^3 + x^2 + 1 (0x11d), the field ISA-L and zfec use.

export const POLY = 0x11d;

/** EXP[i] = 2^i; doubled so EXP[LOG[a] + LOG[b]] needs no modulo. */
export const EXP = new Uint8Array(512);
/** LOG[x] for x in 1..255 (LOG[0] is unused). */
export const LOG = new Uint8Array(256);

{
  let x = 1;
  for (let i = 0; i < 255; i++) {
    EXP[i] = x;
    LOG[x] = i;
    x <<= 1;
    if (x & 0x100) x ^= POLY;
  }
  for (let i = 255; i < 512; i++) EXP[i] = EXP[i - 255];
}

export function mul(a: number, b: number): number {
  return a === 0 || b === 0 ? 0 : EXP[LOG[a] + LOG[b]];
}

export function inv(a: number): number {
  if (a === 0) throw new RangeError("0 has no inverse in GF(2^8)");
  return EXP[255 - LOG[a]];
}

const MUL_ROWS: (Uint8Array | undefined)[] = new Array(256);

/** The 256-entry row c * x for every x, built on first use. */
export function mulRow(c: number): Uint8Array {
  let row = MUL_ROWS[c];
  if (!row) {
    row = new Uint8Array(256);
    for (let x = 1; x < 256; x++) row[x] = mul(c, x);
    MUL_ROWS[c] = row;
  }
  return row;
}

/** dst ^= c * src, byte by byte. */
export function mulAddInto(dst: Uint8Array, src: Uint8Array, c: number): void {
  const n = dst.length;
  if (c === 0) return;
  if (c === 1) {
    for (let i = 0; i < n; i++) dst[i] ^= src[i];
    return;
  }
  const row = mulRow(c);
  for (let i = 0; i < n; i++) dst[i] ^= row[src[i]];
}

/** Inverse of an n x n matrix (row-major, n*n bytes) by Gauss-Jordan; null if singular. */
export function invertMatrix(m: Uint8Array, n: number): Uint8Array | null {
  const a = Uint8Array.from(m);
  const out = new Uint8Array(n * n);
  for (let i = 0; i < n; i++) out[i * n + i] = 1;
  for (let col = 0; col < n; col++) {
    let piv = col;
    while (piv < n && a[piv * n + col] === 0) piv++;
    if (piv === n) return null;
    if (piv !== col) {
      for (let j = 0; j < n; j++) {
        let t = a[col * n + j]; a[col * n + j] = a[piv * n + j]; a[piv * n + j] = t;
        t = out[col * n + j]; out[col * n + j] = out[piv * n + j]; out[piv * n + j] = t;
      }
    }
    const s = inv(a[col * n + col]);
    for (let j = 0; j < n; j++) {
      a[col * n + j] = mul(a[col * n + j], s);
      out[col * n + j] = mul(out[col * n + j], s);
    }
    for (let r = 0; r < n; r++) {
      const f = a[r * n + col];
      if (r === col || f === 0) continue;
      for (let j = 0; j < n; j++) {
        a[r * n + j] ^= mul(f, a[col * n + j]);
        out[r * n + j] ^= mul(f, out[col * n + j]);
      }
    }
  }
  return out;
}
