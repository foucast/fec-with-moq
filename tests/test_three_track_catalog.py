"""The three-track catalog (notes/three-track-catalog.md): a plain LOC track for every player, the
datagram source track and the repair rungs, with the FEC parameters in packagingMetadata; and the
plain track's objects. Runs without an mp4: the media is a synthetic stand-in."""
import asyncio
import json

import pytest

from fecmoq import media
from fecmoq.media import (LOC_PROP_TIMESTAMP, MediaBlockSchedule, PlainTrack, catalog_json,
                          packaging_metadata, parse_packaging_metadata, unpack_loc_object)

N, GOP, GAP = 12, 120, 0.2


class FakeMedia:
    """Mp4Gops's interface over synthetic frames: GOP frames to a group, keyframe first."""
    codec, width, height, fps = "av01.0.04M.08", 640, 360, 60.0

    def __init__(self, path, n, gop=None):
        frames = [(bytes([g, i % 256]) * (50 + 7 * i), i == 0) for g in range(3) for i in range(GOP)]
        self.blocks = [frames[i:i + n] for i in range(0, len(frames), n)]
        self.blocks_per_gop = (gop or n) // n
        self.sizes = [len(p) for p, _ in frames]
        self.largest = max(self.sizes)

    peak_bitrate = media.Mp4Gops.peak_bitrate

    def block(self, i):
        return self.blocks[i % len(self.blocks)]


@pytest.fixture
def schedule(monkeypatch):
    monkeypatch.setattr(media, "Mp4Gops", FakeMedia)
    return MediaBlockSchedule(media="fake.mp4", gop_frames=GOP, seed=1, n_blocks=30, n_objects=N,
                              gap_s=GAP)


def _catalog(sch, plain="video"):
    return json.loads(catalog_json(sch.media, n_objects=N, gap_s=GAP, rungs=[0.15, 0.15, 0.15],
                                   symbol_size=1100, plain=plain))


def test_catalog_has_plain_source_and_repair_tracks(schedule):
    cat = _catalog(schedule)
    assert cat["version"] == "draft-02"
    by = {t["name"]: t for t in cat["tracks"]}
    assert [t["name"] for t in cat["tracks"]] == ["video", "src", "rep1", "rep2", "rep3"]
    assert all("packaging" in t and t["isLive"] is True for t in cat["tracks"])
    v = by["video"]
    assert v["packaging"] == "loc" and v["role"] == "video" and v["codec"] == FakeMedia.codec
    assert v["framerate"] == 60.0 and v["bitrate"] > 0 and "packagingMetadata" not in v
    assert by["src"]["packaging"] == "x-fec-source"
    meta = parse_packaging_metadata(by["src"]["packagingMetadata"])
    assert meta == {"scheme": "rs-cauchy-gf256", "symbolSize": "1100", "blockObjects": "12",
                    "blockSeconds": "0.2", "blocksPerGroup": "10", "objectStride": "256",
                    "rungs": "3", "plainTrack": "video"}
    for r in (1, 2, 3):
        t = by[f"rep{r}"]
        assert t["packaging"] == "x-fec-repair"
        assert parse_packaging_metadata(t["packagingMetadata"]) == {
            "source": "src", "rung": str(r), "fraction": "0.15"}
    # no source track left labelled loc: only the plain track is
    assert [t["name"] for t in cat["tracks"] if t["packaging"] == "loc"] == ["video"]


def test_catalog_without_plain_track(schedule):
    cat = _catalog(schedule, plain=None)
    assert [t["name"] for t in cat["tracks"]] == ["src", "rep1", "rep2", "rep3"]
    assert "plainTrack" not in parse_packaging_metadata(cat["tracks"][0]["packagingMetadata"])


def test_packaging_metadata_syntax():
    assert packaging_metadata({"a": 1, "b": "x|y:z"}) == "a=1;b=x|y:z"
    assert parse_packaging_metadata("a=1;a=2;;junk;b=") == {"a": "1", "b": ""}
    for bad in ({"a b": 1}, {"a": "x;y"}, {"a": "x=y"}):
        with pytest.raises(ValueError):
            packaging_metadata(bad)


def test_peak_bitrate_is_the_busiest_second(schedule):
    sizes = schedule.media.sizes
    w = 60
    loop = sizes + sizes[:w - 1]
    assert schedule.media.peak_bitrate(60.0) == 8 * max(sum(loop[i:i + w]) for i in range(len(sizes)))


class _Out:
    def __init__(self):
        self.writes = []

    async def write(self, group_id, object_id, payload, *, extensions=None, group_start=False):
        self.writes.append((group_id, object_id, payload, extensions, group_start))


def _plain(schedule, n_blocks):
    t = PlainTrack.__new__(PlainTrack)          # no session: produce() only needs the schedule
    t.schedule, t.n_blocks = schedule, n_blocks
    return t


def test_plain_track_carries_the_source_frames(schedule):
    schedule.gap_s = 0.0                         # no pacing in the test
    out = _Out()
    asyncio.run(_plain(schedule, 20).produce(out))
    assert len(out.writes) == 20 * N
    for b in range(20):
        g, q = divmod(b, 10)
        for i, obj in enumerate(schedule.block(b)):
            props, payload = unpack_loc_object(obj)
            wg, wo, wp, ext, start = out.writes[b * N + i]
            # same frame bytes and timestamp as the datagram source's LOC object
            assert (wg, wo, wp) == (g, q * N + i, payload)
            assert ext == {LOC_PROP_TIMESTAMP: schedule.frames(b)[i][0]}
            assert start == (q == 0 and i == 0)
    # object ids contiguous within each group, 0 .. GOP - 1
    for g in (0, 1):
        assert [w[1] for w in out.writes if w[0] == g] == list(range(GOP))


def test_plain_track_joins_at_the_next_gop(schedule):
    import time
    schedule.t_start = time.monotonic() - 3.3 * schedule.gap_s   # block 3 is under way
    assert _plain(schedule, 30)._first_block() == 10
    schedule.t_start = None
    assert _plain(schedule, 30)._first_block() == 0
