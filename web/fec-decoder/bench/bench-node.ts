// Node harness: npm run bench (prints one JSON line per erasure count).
import { readFileSync } from "node:fs";
import { cpus } from "node:os";
import { runBench } from "./bench-core.ts";

const v = JSON.parse(readFileSync(new URL("../../test/vectors.json", import.meta.url), "utf8"));
console.log(JSON.stringify({ runtime: `node ${process.version}`, cpu: cpus()[0]?.model }));
for (const row of runBench(v)) console.log(JSON.stringify(row));
