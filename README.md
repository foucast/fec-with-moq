# FEC with MoQ

Receiver-chosen forward error correction over Media over QUIC (MoQ). A publisher sends a source track plus a ladder of repair tracks. Each subscriber takes as many repair tracks as its own path needs, with ordinary SUBSCRIBE, so only the viewers who need protection pay for it. **The relay is unmodified and never learns that FEC exists.**

Background, in plain language: the FEC with MoQ article series on LinkedIn, [Part 1](https://www.linkedin.com/pulse/fec-moq-marc-fiuczynski-tp3nc/), [Part 2](https://www.linkedin.com/pulse/fec-moq-part-2-each-receiver-picks-its-protection-marc-fiuczynski-hp4lc/) and [Part 3](https://www.linkedin.com/pulse/fec-moq-part-3-ideas-stands-marc-fiuczynski-suhyc/).

## Status

Working end to end: the Python publisher sends real media (source plus repair tracks) through a stock [moqx](https://github.com/openmoq/moqx) relay, and browser players recover lost blocks in stock Chromium and Firefox. The player side is in Playa: branch [`fec-with-moq`](https://github.com/foucast/moq-playa/tree/fec-with-moq) of our fork, which has a demo page with an on-screen FEC counter (see its `FEC-WITH-MOQ.md`). The Shaka Player changes will follow in a fork of Shaka.

Not yet shown: congestion with real cross traffic, bitrate and protection chosen together, content-aware protection, or multicast.

## What's here

| Path | What |
|---|---|
| `src/fecmoq/` | The repair layer in Python: Reed–Solomon coder (`codec.py`), symbol layout and wire format (`symbols.py`, `wire.py`), MoQ publishing and FETCH answers (`moq.py`), media timeline (`media.py`) |
| `scripts/relay_fec.py` | Publisher, measurement subscriber and offline analysis through a relay (`pub`, `sub`, `analyze`) |
| `scripts/loopback_fec.py` | Source plus repair tracks over loopback, no relay |
| `scripts/fetch_*.py`, `scripts/relay_edge_probe.py` | Protocol probes: FETCH by range, FETCH decoding, relay edge cases |
| `scripts/fec_vectors.py`, `scripts/fec_media_vectors.py` | Generate the browser decoder's test vectors |
| `web/fec-decoder/` | The browser decoder (TypeScript, no runtime dependencies); see its README |
| `tests/` | Unit tests |
| `vendor/aiomoqt` | [aiomoqt](https://github.com/gmarzot/aiomoqt) as a git submodule, unmodified |

Each directory has a short README listing what's in it.

## Setup

Needs Python 3.12+ and Intel ISA-L (`sudo apt install libisal-dev`).

```bash
git clone --recursive https://github.com/foucast/fec-with-moq.git
cd fec-with-moq
scripts/setup.sh                    # venv, aiomoqt editable, this package
. .venv/bin/activate
python -m pytest tests/ -q
mkdir -p certs && openssl req -x509 -newkey rsa:2048 -nodes -days 365 -subj /CN=localhost \
  -keyout certs/key.pem -out certs/cert.pem
python scripts/loopback_fec.py      # source + 3 repair tracks over loopback, no relay
```

Through a relay, the tools default to OpenMoQ's public relays, `moqt://lb.moqx.akaleapi.net:443/moq-relay`: the name routes you to the nearest one. The publisher needs raw QUIC (`moqt://`), because aiomoqt can't send datagrams over WebTransport; a subscriber can also use `https://lb.moqx.akaleapi.net/moq-relay`. Pass `--relay` to use your own.

Tested on Ubuntu 24.04. If it doesn't work on your system, please open an issue, and if you can fix it, send a pull request from your fork.

The coder is ISA-L (Cauchy Reed–Solomon). An older zfec backend is kept as an optional comparison; it isn't installed by default because zfec is GPL-2+.

## Design

How the MoQ tracks are laid out (source on subgroup streams, repair rungs on datagrams, FETCH as a fallback) is in [`DESIGN.md`](DESIGN.md). Start there rather than with the code.

### Design choices

1. **Unmodified relay, no protocol change.** Everything runs against the stock moqx image.
2. **The repair layer treats objects as opaque bytes**, so any streaming format can use it. Content-aware protection, if it comes, is a publisher-side decision layered on top.
3. **Reed–Solomon, not RaptorQ.** Reed–Solomon is enough to show the mechanism.

## Contributing

Proposals and tasks will be posted as GitHub issues in this repository.

## Licence

Apache License 2.0 (`LICENSE`). Copyright 2026 Marc E. Fiuczynski (`NOTICE`).

## Credits

Much of this code was written with Claude (Anthropic), working under my direction.
