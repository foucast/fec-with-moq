// Decode timing for an arm-B block (k = 125, 57 repair symbols: rungs of 19), shared by the Node
// and the browser harness. Times recoverSymbols() only: e erased source symbols rebuilt from the
// rest plus e repair symbols. Each result is checked byte for byte once, before timing.

import { recoverSymbols } from "../src/decoder.ts";
import { layoutBlock, toSymbols } from "../src/layout.ts";
import { parseRepairObject } from "../src/wire.ts";

export interface BenchRow { k: number; e: number; reps: number; median_ms: number; p90_ms: number; min_ms: number }

interface VBlock { k: number; m: number; symbol_size: number; sizes: number[]; objects: string[]; repair_objects: string[] }

function b64(s: string): Uint8Array {
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

export function runBench(vectors: { blocks: VBlock[] }, es = [1, 10, 19, 38, 57], reps = 30): BenchRow[] {
  const b = vectors.blocks.find((x) => x.k > 100)!;           // the first arm-B block
  const lay = layoutBlock(b.sizes, b.symbol_size);
  const symbols = toSymbols(lay, b.objects.map(b64));
  const repairAll = b.repair_objects.map((p) => parseRepairObject(b64(p)).symbol);
  const rows: BenchRow[] = [];
  for (const e of es) {
    // erase e source symbols spread over the block; use the first e repair symbols
    const missing = new Set(Array.from({ length: e }, (_, i) => Math.floor((i * b.k) / e)));
    const src = new Map<number, Uint8Array>();
    symbols.forEach((s, j) => { if (!missing.has(j)) src.set(j, s); });
    const rep = new Map<number, Uint8Array>(repairAll.slice(0, e).map((s, j) => [j, s]));
    const first = recoverSymbols(b.k, src, rep);
    if (!first.ok) throw new Error(`e=${e}: ${first.reason}`);
    first.value.forEach((s, j) => {
      for (let i = 0; i < s.length; i++) if (s[i] !== symbols[j][i]) throw new Error(`e=${e}: symbol ${j} differs`);
    });
    const t: number[] = [];
    for (let r = 0; r < reps; r++) {
      const t0 = performance.now();
      recoverSymbols(b.k, src, rep);
      t.push(performance.now() - t0);
    }
    t.sort((x, y) => x - y);
    const q = (p: number) => t[Math.min(t.length - 1, Math.floor(p * t.length))];
    rows.push({ k: b.k, e, reps, median_ms: +q(0.5).toFixed(3), p90_ms: +q(0.9).toFixed(3), min_ms: +t[0].toFixed(3) });
  }
  return rows;
}
