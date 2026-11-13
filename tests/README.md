# tests

Run with `python -m pytest tests/ -q` from the repository root. Tests that need the optional zfec coder skip when it isn't installed.

| File | What it checks |
|---|---|
| `test_roundtrip.py` | Lose whole source objects, reconstruct them from repair |
| `test_codec_backends.py` | The coder properties the protection ladder relies on: any k of k + m symbols decode, repair symbols don't depend on how many were made |
| `test_block_schedule.py` | The block schedule: incremental encoding equals a batch encode, and repair waits for its source |
| `test_media_fec.py` | Real-media blocks survive the symbol path |
| `test_media_timeline.py` | Blocks cut the media timeline independently of frame types |
| `test_fetch_objects.py` | Repair by FETCH, the publisher's side |
| `test_fetch_server.py` | FETCH answered by the publisher |
| `test_aiomoqt_compat.py` | The aiomoqt internals this code hooks into still have the shape it expects |
