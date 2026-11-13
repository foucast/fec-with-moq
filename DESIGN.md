# Design: FEC with MoQ tracks

This describes how FEC with MoQ uses MoQT: which tracks exist, how objects map onto streams and datagrams, and what a receiver does with them. It is about the design, not the code. The code that implements it is in `src/fecmoq/` (mostly `moq.py`, `symbols.py` and `wire.py`).

## The constraint

The relay is unmodified and never learns that FEC exists. Everything here uses ordinary MoQT: tracks, SUBSCRIBE, FETCH, subgroup streams and datagrams. There is no MoQT extension and no new object mapping, and the FEC metadata travels inside object payloads. A relay that can forward and cache tracks can carry FEC with MoQ.

## Tracks

One namespace (`fecmoq` by default) holds:

1. **The source track** (`src`): the media objects themselves, unchanged in meaning. A player that knows nothing about FEC can play it.
2. **The repair tracks** (`rep1`, `rep2`, … `repN`), one per *rung* of a protection ladder.
3. **The catalog** (`catalog`), when publishing real media: an MSF catalog that describes the source track as LOC video and carries the FEC parameters (see "The catalog").

A subscriber chooses its protection by choosing which repair tracks to SUBSCRIBE to, with nothing else: no feedback to the publisher and no per-receiver state in the relay. A viewer on a clean path takes `src` alone; a viewer on a lossy path adds `rep1`, or `rep1` and `rep2`, and so on. Only the viewers who need protection pay for it in bandwidth.

## Blocks and symbols

The source track is cut into **blocks** of consecutive objects (12 by default; with real media, the frames of a GOP or of part of a GOP). Each block is protected on its own.

Within a block, the objects are laid out back to back and cut into fixed-size **symbols** (1100 bytes), and only the last symbol is padded. This gives the one mapping that MoQT doesn't provide: a deterministic map from source objects to symbol indices. A lost source object therefore becomes a known set of erased symbols at the receiver. A symbol that straddles two objects is erased if either object is lost; the alternative, padding every object to whole symbols, avoids that at the cost of padding everywhere.

## The code, and why rungs can be additive

The repair symbols come from a systematic Reed–Solomon code over GF(2^8) with a Cauchy matrix (as in Intel ISA-L): the source symbols are sent as they are, and a block of k source symbols gets up to m repair symbols, with k + m ≤ 256. Any k of the k + m symbols recover the block.

Repair symbol j depends only on j and k, not on how many repair symbols were generated. That is what lets the rungs be **additive**: each rung carries its own disjoint slice of the block's repair symbols (by default each rung adds about 15% of k), and a subscriber on rungs 1 to r holds the union of their slices. Because any symbols will do, it doesn't matter which rungs a symbol came from, only how many arrived. The block is encoded once, and every rung sends its slice of that one encode.

## Mapping onto MoQT

1. **Source objects ride subgroup streams**, by default one stream per MoQ group. They are reliable: QUIC retransmits them, so they eventually arrive.
2. **Repair symbols ride datagrams**, one repair symbol per datagram and per object. They are never retransmitted: a lost repair datagram is simply gone, and the receiver uses whatever repair did arrive. Every repair object fits in a single datagram, so nothing is fragmented.
3. **Groups and object ids.** A MoQ group is a block (with real media, a GOP, which may hold several blocks). Within a group, an object's id is its symbol index, offset per block when a group holds more than one. On the repair tracks, the object id is the symbol's global repair index, so a receiver knows which repair symbol it holds without parsing the payload. An alternative numbers each rung's objects from 0 and carries the global index in the payload instead, so that every rung's group starts at object 0 for relay caches.

The code also supports other source mappings for comparison, for example the source block itself as symbols on datagrams, or one stream per object, so that a lost packet holds back only its own object. The mapping above is the default.

## What "lost" means

Because source objects are reliable, they always arrive eventually. What matters for live media is whether they arrive in time. In the measurement tools, a source object counts as lost if it hasn't arrived by a **deadline**, measured from when its block started at that receiver (the tools offer a few definitions of that start, e.g. the block's first arrival of any object). Repair turns late or missing source objects into recoverable erasures before the deadline, instead of waiting for retransmission.

## Order on the wire: source first

Repair for a block is only useful after its source, and MoQT priority doesn't cover datagrams. So the publisher itself makes repair rank below source: a block's repair leaves only after that block's source has left the publisher. Without this, repair datagrams for a block can overtake its own source on the wire and delay it.

## Repair format, in the payload

Every repair object carries the sizes of all objects in its block:

    codec:u8 | n_objects:u8 | size[0]:u16 … size[n-1]:u16 | [index:u16] | symbol

A receiver can therefore rebuild the symbol layout from any single repair object, even when the source objects that would have revealed those sizes are exactly the ones that were lost. The `codec` byte names the code that produced the symbol (receivers refuse a mismatch rather than decode garbage), and its high bit marks the form that carries the global repair index. This is deliberately not in MoQT object extension headers: keeping it in the payload uses only the most basic wire features, which is the strongest form of the "no protocol change" claim.

## Repair by FETCH, when FEC isn't enough

If a block still can't be recovered from the repair that arrived, the receiver can FETCH what it is missing (source objects or more repair symbols) from the relay's cache, using ordinary MoQT FETCH. The relay answers from its cache or asks the publisher, which serves FETCH for the source track and every rung over a recent window. Two things follow: the relay must cache the tracks, and how long it keeps them bounds how late a repair can still be fetched.

## The catalog

With real media, the publisher sends an MSF catalog. The source track is listed as LOC video that an ordinary player can configure a decoder from, with the FEC parameters attached (code, symbol size, objects per block, rung names and fractions, and how objects map onto groups). The repair tracks are listed under an unregistered packaging (`x-fec-repair`), so a player without FEC support skips them.

## Not covered yet

Choosing bitrate and protection together, content-aware protection (protecting keyframes more than frames nothing depends on), behaviour under congestion with real cross traffic, and multicast.
