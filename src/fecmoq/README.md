# fecmoq

The repair layer, in Python. How the pieces fit together on the wire is in [`DESIGN.md`](../../DESIGN.md).

| File | What |
|---|---|
| `codec.py` | The erasure coder: Reed–Solomon over GF(2^8) via Intel ISA-L (default), plus an optional zfec backend for comparison |
| `symbols.py` | Maps a block's source objects to fixed-size symbols, so a lost object becomes a known set of erased symbols |
| `wire.py` | The repair-object format: block object sizes, coder id, optional global index, then the symbol |
| `moq.py` | The MoQ side, built on aiomoqt: the block schedule, source and repair tracks, and FETCH answers |
| `media.py` | Real media (LOC video frames from an mp4) as source objects, the plain track, and the three-track MSF catalog with the FEC parameters |
| `ts.py` | MPEG-TS: packets to objects and groups, PSI (PAT/PMT and SCTE-35 PIDs), PCR timing, the subscriber's reassembler and TS output |
| `ts_moq.py` | MPEG-TS on the MoQ side: the TS block schedule (a file paced by PCR, or a live source) and the source track |
| `arrivals.py` | The subscriber's per-object arrival records, stored as columns |
| `synth.py` | Synthetic source objects with a realistic size distribution, for runs without media |
