# scripts

Run from the repository root, after `scripts/setup.sh`.

| Script | What |
|---|---|
| `setup.sh` | One-shot setup: virtual environment, aiomoqt (editable, from the submodule), this package |
| `loopback_fec.py` | Source plus repair tracks over real MoQT on one machine, no relay. The quickest way to see it work |
| `relay_fec.py` | The main tool, through a relay: `pub` publishes source and repair tracks, `sub` subscribes and logs every arrival, `analyze` reconstructs blocks offline at chosen deadlines |
| `pub_media_mapped.py` | aiomoqt's LOC media publisher with one stream per object, for the no-FEC baseline |
| `fetch_check.py` | Checks that FETCH works through a relay for these tracks |
| `fetch_range.py` | FETCH through a relay, by range or joining, checked object by object |
| `fetch_decode.py` | An independent decoder for MoQT FETCH streams, for tracking down payload corruption |
| `relay_edge_probe.py` | Probes relay edge cases: object-id gaps, extensions, end of group, session end |
| `fec_vectors.py` | Generates test vectors for the browser decoder (`web/fec-decoder/test/vectors.json`) |
| `fec_media_vectors.py` | Generates real-media test vectors for the browser decoder |
| `bench_coders.py` | Encode and decode time per block for each coder, no network |
