// Repair-object wire format (src/fecmoq/wire.py):
//
//   codec:u8 | n_objects:u8 | size[0]:u16 ... size[n-1]:u16 | symbol
//   codec|0x80:u8 | n_objects:u8 | size[0]:u16 ... size[n-1]:u16 | index:u16 | symbol   (INDEXED)
//
// Big-endian. codec 1 = zfec, 2 = ISA-L. Every repair object carries the block's object sizes, so
// one repair object is enough to rebuild the symbol layout. INDEXED objects carry the global repair
// index; otherwise the MoQ object id is the index.

export const CODEC_ZFEC = 1;
export const CODEC_ISAL = 2;
export const INDEXED = 0x80;

export interface RepairObject {
  codec: number;
  indexed: boolean;
  sizes: number[];
  /** The global repair index carried in the payload (INDEXED form), else null. */
  index: number | null;
  symbol: Uint8Array;
}

export function parseRepairObject(p: Uint8Array): RepairObject {
  if (p.length < 2) throw new RangeError(`repair object too short (${p.length} B)`);
  const codec = p[0] & ~INDEXED;
  if (codec !== CODEC_ZFEC && codec !== CODEC_ISAL) throw new RangeError(`unknown repair codec id ${codec}`);
  const indexed = (p[0] & INDEXED) !== 0;
  const n = p[1];
  const header = 2 + 2 * n + (indexed ? 2 : 0);
  if (p.length < header) throw new RangeError(`repair object header needs ${header} B, got ${p.length}`);
  const sizes: number[] = [];
  for (let i = 0; i < n; i++) sizes.push((p[2 + 2 * i] << 8) | p[3 + 2 * i]);
  const index = indexed ? (p[2 + 2 * n] << 8) | p[3 + 2 * n] : null;
  return { codec, indexed, sizes, index, symbol: p.subarray(header) };
}
