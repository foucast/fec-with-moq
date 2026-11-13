// Decoder for the ISA-L backend of src/fecmoq/codec.py: a systematic Cauchy code over GF(2^8).
// Repair symbol j of a block with k source symbols is matrix row i = k + j, whose coefficient for
// source column c is gf_inv(i XOR c) (ISA-L gf_gen_cauchy1_matrix). Row k + j depends only on
// (j, k), so a symbol is the same whatever total number of repair symbols was generated.
//
// Decoding solves only for the e erased source symbols, as codec.py does: take the e lowest-indexed
// repair symbols received, compute the syndrome s = repair + C[R, known] * known, then
// missing = inverse(C[R, missing]) * s.

import { inv, invertMatrix, mulAddInto } from "./gf256.ts";
import { availableSymbols, fromSymbols, layoutBlock, SYMBOL_SIZE } from "./layout.ts";
import { CODEC_ISAL, parseRepairObject } from "./wire.ts";

export type Result<T> = { ok: true; value: T } | { ok: false; reason: string };

/** Coefficient of source column c in repair symbol j of a block of k source symbols. */
export function cauchy(k: number, j: number, c: number): number {
  return inv((k + j) ^ c);
}

/**
 * Recover all k source symbols from those received plus repair symbols (global repair index ->
 * symbol). Every symbol has the same length. Fails, without throwing, when fewer repair symbols
 * than erasures arrived.
 */
export function recoverSymbols(
  k: number,
  source: Map<number, Uint8Array>,
  repair: Map<number, Uint8Array>,
): Result<Uint8Array[]> {
  const missing: number[] = [];
  const known: number[] = [];
  for (let c = 0; c < k; c++) (source.has(c) ? known : missing).push(c);
  const e = missing.length;
  if (e === 0) return { ok: true, value: known.map((c) => source.get(c)!) };
  const rows = [...repair.keys()].sort((a, b) => a - b).slice(0, e);
  if (rows.length < e) {
    return { ok: false, reason: `${e} source symbols missing, only ${rows.length} repair symbols` };
  }
  if (k + rows[e - 1] > 255) return { ok: false, reason: "repair index beyond GF(2^8) (k + j > 255)" };
  const n = repair.get(rows[0])!.length;

  // e x e block of the Cauchy rows on the missing columns, inverted
  const sq = new Uint8Array(e * e);
  rows.forEach((j, r) => missing.forEach((c, m) => (sq[r * e + m] = cauchy(k, j, c))));
  const invSq = invertMatrix(sq, e);
  if (!invSq) return { ok: false, reason: "singular matrix (cannot happen for a Cauchy code)" };

  // syndrome: repair + C[R, known] * known
  const syn = rows.map((j) => {
    const s = Uint8Array.from(repair.get(j)!);
    for (const c of known) mulAddInto(s, source.get(c)!, cauchy(k, j, c));
    return s;
  });

  // missing = inverse * syndrome
  const out = new Map<number, Uint8Array>();
  missing.forEach((c, m) => {
    const x = new Uint8Array(n);
    for (let r = 0; r < e; r++) mulAddInto(x, syn[r], invSq[m * e + r]);
    out.set(c, x);
  });
  const all: Uint8Array[] = [];
  for (let c = 0; c < k; c++) all.push(source.get(c) ?? out.get(c)!);
  return { ok: true, value: all };
}

export interface BlockInput {
  /** The block's object sizes (from any repair object of the block). */
  sizes: number[];
  /** Source objects received, by index in the block. */
  objects: Map<number, Uint8Array>;
  /** Repair objects received as they arrive on the wire, with their global repair index. */
  repair: Map<number, Uint8Array>;
  symbolSize?: number;
}

/** Rebuild every object of a block from the source objects and repair objects that arrived. */
export function decodeBlock(b: BlockInput): Result<Uint8Array[]> {
  const lay = layoutBlock(b.sizes, b.symbolSize ?? SYMBOL_SIZE);
  const symbols = new Map<number, Uint8Array>();
  for (const [j, payload] of b.repair) {
    const r = parseRepairObject(payload);
    if (r.codec !== CODEC_ISAL) return { ok: false, reason: `repair codec ${r.codec}, this decoder is ISA-L (2)` };
    if (r.symbol.length !== lay.symbolSize) {
      return { ok: false, reason: `repair symbol ${r.symbol.length} B, layout expects ${lay.symbolSize} B` };
    }
    symbols.set(r.index ?? j, r.symbol);
  }
  const rec = recoverSymbols(lay.k, availableSymbols(lay, b.objects), symbols);
  return rec.ok ? { ok: true, value: fromSymbols(lay, rec.value) } : rec;
}
