# FEC with MoQ

Receiver-chosen forward error correction over Media over QUIC (MoQ). A publisher sends a source track plus a ladder of repair tracks. Each subscriber takes as many repair tracks as its own path needs, with ordinary SUBSCRIBE, so only the viewers who need protection pay for it. **The relay is unmodified and never learns that FEC exists.**

Background, in plain language: the FEC with MoQ article series on LinkedIn, [Part 1](https://www.linkedin.com/pulse/fec-moq-marc-fiuczynski-tp3nc/), [Part 2](https://www.linkedin.com/pulse/fec-moq-part-2-each-receiver-picks-its-protection-marc-fiuczynski-hp4lc/) and [Part 3](https://www.linkedin.com/pulse/fec-moq-part-3-ideas-stands-marc-fiuczynski-suhyc/).

## Status

Working end to end: the Python publisher sends real media (source plus repair tracks) through a stock [moqx](https://github.com/openmoq/moqx) relay, and browser players recover lost blocks in stock Chromium and Firefox. The player side is in Playa: branch [`fec-with-moq-v2`](https://github.com/foucast/moq-playa/tree/fec-with-moq-v2) of our fork, on current upstream Playa, with a demo page and an on-screen FEC counter (see its `FEC-WITH-MOQ.md`). The older branch [`fec-with-moq`](https://github.com/foucast/moq-playa/tree/fec-with-moq) (Nov 13) reads the earlier catalog and won't play this publisher's. The Shaka Player changes will follow in a fork of Shaka.

Tested (Nov 22, 2026) with current upstream Playa, moqx 0.3.8+ (`a191e99`) and the public OpenMOQ CI relay. Relays older than moqx 0.3.8 end a WebTransport session that only receives datagrams after 60 s (fixed upstream in [moqx #882](https://github.com/openmoq/moqx/pull/882)), which cuts off a FEC player.

New with this update: **MPEG-TS in and out** (below). A transport stream file, a live source or GPAC's test generator goes in as the source track with FEC on top, and the subscriber writes the TS back out; tested (Nov 23, 2026) through moqx `a191e99` and the public OpenMOQ CI relay, `test60.ts` arriving byte-identical.

## The catalog: one plain track for every player, plus FEC tracks

The publisher's MSF catalog (version `draft-02`) has three kinds of track, so players without FEC keep working:

| Track | Packaging | Carried as | Who subscribes |
|---|---|---|---|
| `video` | `loc` | QUIC streams, one stream per group, one frame per object | any player without FEC |
| `src` | `x-fec-source` | datagrams: the same frames as FEC source symbols | FEC players |
| `rep1`..`rep3` | `x-fec-repair` | datagrams: repair, one rung per track | FEC players, as many rungs as their path needs |

The FEC parameters are in each track's `packagingMetadata` (`key=value;...`, MSF-02 §5.2.5), for example (abridged) `scheme=rs-cauchy-gf256;symbolSize=1100;blockObjects=12;rungs=3;plainTrack=video` on `src` and `source=src;rung=1;fraction=0.15` on a repair track. The two `x-` packaging names are placeholders until MSF has a packaging registry. A FEC player selects `video` as usual, then subscribes to `src` and the repair tracks in its place. The relay forwards each viewer only the tracks it asked for, so a plain viewer receives no FEC bytes. `relay_fec.py pub --no-plain` leaves the plain track out, for comparison runs.

Not yet shown: congestion with real cross traffic, bitrate and protection chosen together, content-aware protection, or multicast.

## What's here

| Path | What |
|---|---|
| `src/fecmoq/` | The repair layer in Python: Reed–Solomon coder (`codec.py`), symbol layout and wire format (`symbols.py`, `wire.py`), MoQ publishing and FETCH answers (`moq.py`), media timeline and catalog (`media.py`), MPEG-TS objects and reassembly (`ts.py`, `ts_moq.py`), the subscriber's arrival records (`arrivals.py`) |
| `scripts/relay_fec.py` | Publisher, measurement subscriber and offline analysis through a relay (`pub`, `sub`, `analyze`), for synthetic blocks, real media or MPEG-TS |
| `scripts/loopback_fec.py` | Source plus repair tracks over loopback, no relay |
| `scripts/fetch_*.py`, `scripts/relay_edge_probe.py` | Protocol probes: FETCH by range, FETCH decoding, relay edge cases |
| `scripts/fec_vectors.py`, `scripts/fec_media_vectors.py` | Generate the browser decoder's test vectors |
| `web/fec-decoder/` | The browser decoder (TypeScript, no runtime dependencies); see its README |
| `tests/` | Unit tests |
| `vendor/aiomoqt` | [aiomoqt](https://github.com/gmarzot/aiomoqt) as a git submodule, unmodified |

Each directory has a short README listing what's in it.

## Setup

Needs Python 3.12+ and Intel ISA-L (`sudo apt install libisal-dev`). For MPEG-TS: GPAC 2.2 with its Python bindings for a live or generated source (`sudo apt install gpac`; a TS file needs nothing extra).

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

Through a relay, the tools default to OpenMoQ's public relays, `moqt://lb.moqx.akaleapi.net:443/moq-relay`: the name routes you to the nearest one. The publisher needs raw QUIC (`moqt://`), because aiomoqt can't send datagrams over WebTransport; a subscriber can also use `https://lb.moqx.akaleapi.net/moq-relay`. Pass `--relay` to use your own. Use a fresh namespace (`--ns`) per run: the relays are shared.

Tested on Ubuntu 24.04. If it doesn't work on your system, please open an issue, and if you can fix it, send a pull request from your fork.

The coder is ISA-L (Cauchy Reed–Solomon). An older zfec backend is kept as an optional comparison; it isn't installed by default because zfec is GPL-2+.

## MPEG-TS in and out

An MPEG transport stream travels as the source track, in objects of whole TS packets (draft-gregoire-moq-msfts-02; a MoQ group per random access point), with repair tracks as for any source. The subscriber writes it back out after FEC, and FETCH where FEC wasn't enough.

```bash
# test vector: 60 s of GPAC's generator, H.264 + AAC, CBR 6 Mb/s
gpac -i avgen:dur=60 c=avc:b=5000000:fintra=1:g=25 c=aac:b=128000 -o test60.ts:rate=6000000
# publisher: a TS file, paced by its PCR (or --ts avgen --ts-dur 120 for a live source, or --ts udp://:PORT)
python scripts/relay_fec.py pub --ns $NS --gate-file /tmp/gate --ts test60.ts \
  --source-mapping datagram --repair-mapping datagram --objects 24 --rungs 0.15,0.15,0.15 --keepalive 5 --out run
# subscriber: the TS after FEC and FETCH, to a file (or - for stdout, or udp://host:port)
python scripts/relay_fec.py sub --ns $NS --objects 24 --rungs 0.15,0.15,0.15 --take 3 --label sub \
  --duration 80 --ts-out out.ts --out run
# once the subscriber prints SUBSCRIBED: touch /tmp/gate on the publisher; at the end
cmp out.ts test60.ts
```

- A TS file is carried unmodified: the output is byte-identical to the input when nothing is lost for good. `--ts-psi-each-group` starts each group with copies of the last PAT and PMT, so a joiner can decode at once: `insert` adds them (2 packets per group); `replace` puts them in place of null packets near the group start, keeping the rate and every PCR's position. Both are off by default for a file or a URL source; `insert` is the default for `avgen`.
- `--source-mapping datagram` (source objects as datagrams, FEC + FETCH) gives the lowest lateness under random loss; `group` (a stream per group) is the choice for bursty loss with a long window.
- A publisher whose relay session dies stops and exits with status 4 (`SESSION-DEAD`) instead of queueing; `--keepalive` keeps a session up while it waits for the gate.

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
