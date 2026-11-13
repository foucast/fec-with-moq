# fecmoq

The repair layer, in Python. How the pieces fit together on the wire is in [`DESIGN.md`](../../DESIGN.md).

| File | What |
|---|---|
| `codec.py` | The erasure coder: Reed–Solomon over GF(2^8) via Intel ISA-L (default), plus an optional zfec backend for comparison |
| `symbols.py` | Maps a block's source objects to fixed-size symbols, so a lost object becomes a known set of erased symbols |
| `wire.py` | The repair-object format: block object sizes, coder id, optional global index, then the symbol |
| `moq.py` | The MoQ side, built on aiomoqt: the block schedule, source and repair tracks, and FETCH answers |
| `media.py` | Real media (LOC video frames from an mp4) as source objects, and the MSF catalog with the FEC parameters |
| `synth.py` | Synthetic source objects with a realistic size distribution, for runs without media |
