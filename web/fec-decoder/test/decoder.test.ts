// Decodes the vectors scripts/fec_vectors.py wrote from our Python ISA-L encoder, and checks the
// result is byte-identical to the original objects (or fails cleanly where the case is undecodable).
//
//   node --test test/decoder.test.ts

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { decodeBlock, recoverSymbols } from "../src/decoder.ts";
import { fromSymbols, layoutBlock, toSymbols } from "../src/layout.ts";
import { parseRepairObject } from "../src/wire.ts";

interface Case {
  name: string;
  missing_symbols: number[];
  repair_available: number[];
  expect: "ok" | "fail";
  lost_objects?: number[];
}
interface Block {
  name: string; k: number; m: number; symbol_size: number; sizes: number[];
  objects: string[]; repair_objects: string[]; cases: Case[];
}
interface Wire {
  name: string; object: string; codec: number; indexed: boolean; n_objects: number;
  sizes: number[]; index: number | null; symbol: string; length: number;
}

const V = JSON.parse(readFileSync(new URL("./vectors.json", import.meta.url), "utf8")) as
  { blocks: Block[]; wire: Wire[] };
const bytes = (s: string) => new Uint8Array(Buffer.from(s, "base64"));

for (const b of V.blocks) {
  const objects = b.objects.map(bytes);
  const repairObjects = b.repair_objects.map(bytes);
  const lay = layoutBlock(b.sizes, b.symbol_size);

  test(`${b.name}: layout gives k = ${b.k}`, () => assert.equal(lay.k, b.k));

  for (const c of b.cases) {
    test(`${b.name}: ${c.name} -> ${c.expect}`, () => {
      let res;
      if (c.lost_objects) {
        // object level, repair as it arrives on the wire
        const got = new Map<number, Uint8Array>();
        objects.forEach((o, i) => { if (!c.lost_objects!.includes(i)) got.set(i, o); });
        const rep = new Map<number, Uint8Array>(c.repair_available.map((j) => [j, repairObjects[j]]));
        res = decodeBlock({ sizes: b.sizes, objects: got, repair: rep, symbolSize: b.symbol_size });
        if (res.ok) res.value.forEach((o, i) => assert.deepEqual(o, objects[i], `object ${i}`));
      } else {
        // symbol level
        const symbols = toSymbols(lay, objects);
        const src = new Map<number, Uint8Array>();
        symbols.forEach((s, j) => { if (!c.missing_symbols.includes(j)) src.set(j, s); });
        const rep = new Map<number, Uint8Array>(
          c.repair_available.map((j) => [j, parseRepairObject(repairObjects[j]).symbol]));
        res = recoverSymbols(b.k, src, rep);
        if (res.ok) fromSymbols(lay, res.value).forEach((o, i) => assert.deepEqual(o, objects[i], `object ${i}`));
      }
      assert.equal(res.ok, c.expect === "ok", res.ok ? "decoded" : res.reason);
    });
  }
}

for (const w of V.wire) {
  test(`wire: ${w.name} repair object (${w.length} B) parses to the generator's fields`, () => {
    const p = bytes(w.object);
    assert.equal(p.length, w.length);
    const r = parseRepairObject(p);
    assert.equal(r.codec, w.codec);
    assert.equal(r.indexed, w.indexed);
    assert.equal(r.sizes.length, w.n_objects);
    assert.deepEqual(r.sizes, w.sizes);
    assert.equal(r.index, w.index);
    assert.deepEqual(r.symbol, bytes(w.symbol));
  });
}

test("wire: unknown codec and truncated header are rejected", () => {
  const p = bytes(V.wire[0].object);
  assert.throws(() => parseRepairObject(Uint8Array.of(7, ...p.subarray(1))), /unknown repair codec/);
  assert.throws(() => parseRepairObject(p.subarray(0, 10)), /header needs/);
});

test("a zfec repair object is refused, not mis-decoded", () => {
  const b = V.blocks[0];
  const zfec = bytes(b.repair_objects[0]);
  zfec[0] = 1;
  const objects = b.objects.map(bytes);
  const got = new Map<number, Uint8Array>(objects.slice(1).map((o, i) => [i + 1, o]));
  const res = decodeBlock({ sizes: b.sizes, objects: got, repair: new Map([[0, zfec]]) });
  assert.equal(res.ok, false);
});

test("INDEXED repair objects use the index they carry, not the map key", () => {
  const b = V.blocks[2];
  const objects = b.objects.map(bytes);
  const lay = layoutBlock(b.sizes, b.symbol_size);
  const lost = 0;                                     // the I-frame-sized object: several symbols
  const first = Math.floor(lay.starts[lost] / lay.symbolSize);
  const last = Math.floor((lay.starts[lost] + b.sizes[lost] - 1) / lay.symbolSize);
  const e = last - first + 1;
  const n = b.sizes.length;
  const repair = new Map<number, Uint8Array>();
  for (let j = 0; j < e; j++) {
    const sym = parseRepairObject(bytes(b.repair_objects[j])).symbol;
    const p = new Uint8Array(2 + 2 * n + 2 + sym.length);
    p[0] = 0x82; p[1] = n;
    b.sizes.forEach((s, i) => { p[2 + 2 * i] = s >> 8; p[3 + 2 * i] = s & 0xff; });
    p[2 + 2 * n] = j >> 8; p[3 + 2 * n] = j & 0xff;
    p.set(sym, 4 + 2 * n);
    repair.set(1000 + j, p);                          // keys that are not the repair index
  }
  const got = new Map<number, Uint8Array>();
  objects.forEach((o, i) => { if (i !== lost) got.set(i, o); });
  const res = decodeBlock({ sizes: b.sizes, objects: got, repair });
  assert.ok(res.ok, res.ok ? "" : res.reason);
  if (res.ok) assert.deepEqual(res.value[lost], objects[lost]);
});
