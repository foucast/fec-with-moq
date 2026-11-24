"""The subscriber's columnar arrival records (fecmoq.arrivals) read back as the tuples they replace."""
import pickle

from fecmoq.arrivals import Arrivals, Size


def _recs():
    return [("src", 3, 0, "datagram", 1.25, Size(1128)), ("rep1", 3, 24, "datagram", 1.5, Size(1150)),
            ("src", 4, 1, "stream", 2.0, Size(940))]


def test_records_read_back_as_tuples():
    a = Arrivals()
    for r in _recs():
        a.append(r)
    assert list(a) == _recs() and len(a) == 3
    assert a[1] == _recs()[1] and a[-1] == _recs()[-1] and a[0:2] == _recs()[:2]
    assert [len(r[5]) for r in a] == [1128, 1150, 940]
    assert max(r[4] for r in a) == 2.0


def test_pickles_as_columns_and_back():
    a = Arrivals()
    for r in _recs() * 1000:
        a.append(r)
    b = pickle.loads(pickle.dumps(dict(meta={}, recs=a)))["recs"]
    assert list(b) == list(a)
    b.append(("rep2", 9, 1, "datagram", 3.0, Size(5)))       # names still map after loading
    assert b[-1][0] == "rep2" and b.names.count("rep2") == 1
    # columns, not tuples: well under 40 B per record pickled
    assert len(pickle.dumps(a)) < 40 * len(a)


def test_keeps_payloads_when_asked():
    a = Arrivals(keep_payloads=True)
    a.append(("src", 0, 0, "datagram", 0.5, b"\x47abc"))
    assert a[0][5] == b"\x47abc"
