// SPAN symbol layout (src/fecmoq/symbols.py): a block's objects are packed back to back and cut
// into k = ceil(total / symbolSize) symbols; only the last symbol is zero-padded. A lost object
// erases every symbol it touches, including symbols it shares with a neighbour.

export const SYMBOL_SIZE = 1100;

export interface Layout {
  symbolSize: number;
  sizes: number[];
  starts: number[];
  k: number;
}

export function layoutBlock(sizes: number[], symbolSize = SYMBOL_SIZE): Layout {
  if (sizes.some((s) => s <= 0)) throw new RangeError("object sizes must be positive");
  const starts: number[] = [];
  let off = 0;
  for (const s of sizes) {
    starts.push(off);
    off += s;
  }
  return { symbolSize, sizes, starts, k: Math.ceil(off / symbolSize) };
}

/** Indices of the source symbols object i touches. */
export function symbolsForObject(lay: Layout, i: number): number[] {
  const s = lay.symbolSize;
  const first = Math.floor(lay.starts[i] / s);
  const last = Math.floor((lay.starts[i] + lay.sizes[i] - 1) / s);
  const out: number[] = [];
  for (let j = first; j <= last; j++) out.push(j);
  return out;
}

/** The whole padded block as one buffer, from all objects (encoder side; used by tests). */
export function toSymbols(lay: Layout, objects: Uint8Array[]): Uint8Array[] {
  const buf = new Uint8Array(lay.k * lay.symbolSize);
  objects.forEach((o, i) => buf.set(o, lay.starts[i]));
  const out: Uint8Array[] = [];
  for (let j = 0; j < lay.k; j++) out.push(buf.subarray(j * lay.symbolSize, (j + 1) * lay.symbolSize));
  return out;
}

/** The source symbols that are complete given the objects received (index -> payload). */
export function availableSymbols(lay: Layout, received: Map<number, Uint8Array>): Map<number, Uint8Array> {
  const s = lay.symbolSize;
  const buf = new Uint8Array(lay.k * s);
  const have = new Uint8Array(lay.k).fill(1);
  lay.sizes.forEach((size, i) => {
    const o = received.get(i);
    if (o && o.length === size) buf.set(o, lay.starts[i]);
    else for (const j of symbolsForObject(lay, i)) have[j] = 0;
  });
  const out = new Map<number, Uint8Array>();
  for (let j = 0; j < lay.k; j++) if (have[j]) out.set(j, buf.subarray(j * s, (j + 1) * s));
  return out;
}

/** Cut the k source symbols back into the block's objects. */
export function fromSymbols(lay: Layout, symbols: Uint8Array[]): Uint8Array[] {
  const s = lay.symbolSize;
  const buf = new Uint8Array(lay.k * s);
  symbols.forEach((sym, j) => buf.set(sym, j * s));
  return lay.sizes.map((size, i) => buf.slice(lay.starts[i], lay.starts[i] + size));
}
