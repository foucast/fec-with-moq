"""make_block's media timeline (gop > 0): blocks cut one stream, independent of frame types."""
from fecmoq.moq import make_block
from fecmoq.synth import P_HI, P_LO


def test_gop0_is_the_old_behaviour():
    a = make_block(7, 5, 12)
    assert a == make_block(7, 5, 12, 0)
    assert len(a[0]) > P_HI                      # an I-frame-sized object leads the block


def test_short_blocks_cut_the_same_stream():
    stream12 = [o for b in range(4) for o in make_block(7, b, 12, gop=12)]
    stream3 = [o for b in range(16) for o in make_block(7, b, 3, gop=12)]
    stream6 = [o for b in range(8) for o in make_block(7, b, 6, gop=12)]
    assert stream12 == stream3 == stream6         # identical media whatever the block size
    big = [j for j, o in enumerate(stream3) if len(o) > P_HI]
    assert big == [0, 12, 24, 36]                 # I-frames every 12 objects, not every block
    assert all(P_LO <= len(o) <= P_HI for j, o in enumerate(stream3) if j % 12)
