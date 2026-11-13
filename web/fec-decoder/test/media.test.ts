// Shaka phase 2, stage B check in the TS decoder: real LOC frames (scripts/fec_media_vectors.py, from
// the FEC publisher's MediaBlockSchedule) rebuilt from source-symbol and repair datagrams after
// erasures, then split back into props_len:u16 | properties | payload.
//
//   node --test test/media.test.ts

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { recoverSymbols } from "../src/decoder.ts";
import { fromSymbols, layoutBlock, symbolsForObject } from "../src/layout.ts";
import { parseRepairObject } from "../src/wire.ts";

interface Case { name: string; expect: "ok" | "fail"; repair_available: number[];
  lost_objects?: number[]; missing_symbols?: number[] }
interface MBlock { block: number; k: number; m: number; sizes: number[]; objects: string[];
  frames: string[]; source_objects: string[]; repair_objects: string[]; cases: Case[] }

// media-vectors.json: blocks that are whole GOPs (keyint 12); media-vectors-gop120.json: the first
// six 12-frame blocks of a 120-frame GOP, so one key-frame block and five without.
const load = (f: string) => JSON.parse(readFileSync(new URL(f, import.meta.url), "utf8")) as
  { codec: string; blocks: MBlock[] };
const SETS = [["", load("./media-vectors.json")], ["gop120 ", load("./media-vectors-gop120.json")]] as const;
const bytes = (s: string) => new Uint8Array(Buffer.from(s, "base64"));

/** props_len:u16 | properties | payload -> payload */
function payloadOf(o: Uint8Array): Uint8Array {
  return o.subarray(2 + ((o[0] << 8) | o[1]));
}

for (const [tag, V] of SETS) for (const b of V.blocks) {
  const lay = layoutBlock(b.sizes);
  test(`${tag}media block ${b.block}: sizes from any source datagram give k = ${b.k}`, () => {
    const r = parseRepairObject(bytes(b.source_objects[0]));
    assert.deepEqual(r.sizes, b.sizes);
    assert.equal(layoutBlock(r.sizes).k, b.k);
  });
  for (const c of b.cases) {
    test(`${tag}media block ${b.block}: ${c.name} -> ${c.expect}`, () => {
      const missing = new Set(c.missing_symbols ??
        (c.lost_objects ?? []).flatMap((i) => symbolsForObject(lay, i)));
      const src = new Map<number, Uint8Array>();
      b.source_objects.forEach((p, j) => { if (!missing.has(j)) src.set(j, parseRepairObject(bytes(p)).symbol); });
      const rep = new Map<number, Uint8Array>(
        c.repair_available.map((j) => [j, parseRepairObject(bytes(b.repair_objects[j])).symbol]));
      const res = recoverSymbols(b.k, src, rep);
      assert.equal(res.ok, c.expect === "ok", res.ok ? "decoded" : res.reason);
      if (!res.ok) return;
      const objs = fromSymbols(lay, res.value);
      objs.forEach((o, i) => {
        assert.deepEqual(o, bytes(b.objects[i]), `object ${i}`);
        assert.deepEqual(payloadOf(o), bytes(b.frames[i]), `frame ${i}`);
      });
    });
  }
}
