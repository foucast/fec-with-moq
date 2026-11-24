"""A subscriber's per-object arrival records, stored as columns.

Each record is (track, group, object, mode, t_mono, payload); the stall analysis and the
analyze scripts read them back as such tuples. As a list of tuples a record costs about
200 B (the tuple, a float, two ints and the payload's size object); here it costs 26 B with
sizes only (array columns, track and mode as indexes into short name lists), so a long run at
ten-odd Mb/s holds tens of MB instead of hundreds, and pickles as a few arrays.
"""
from __future__ import annotations

from array import array


class Size(int):
    """An arrival's payload size in place of its payload (--arrival-sizes): len() works."""

    def __len__(self):
        return int(self)


class Arrivals:
    """Append-only (track, group, object, mode, t_mono, payload) records. Iterates, indexes
    and len()s like the list of tuples it replaces. With keep_payloads=False only each
    payload's size is kept, and records give it back as a Size."""

    def __init__(self, keep_payloads: bool = False):
        self.names: list[str] = []          # track names, then modes: indexes in the B columns
        self._ix: dict[str, int] = {}
        self.track = array("B")
        self.mode = array("B")
        self.group = array("q")
        self.obj = array("q")
        self.t = array("d")
        self.size = array("I")
        self.payloads: list | None = [] if keep_payloads else None

    def _name(self, s: str) -> int:
        i = self._ix.get(s)
        if i is None:
            i = self._ix[s] = len(self.names)
            self.names.append(s)
        return i

    def append(self, rec) -> None:
        track, group, obj, mode, t, payload = rec
        self.track.append(self._name(track))
        self.mode.append(self._name(mode))
        self.group.append(group)
        self.obj.append(obj)
        self.t.append(t)
        self.size.append(len(payload))
        if self.payloads is not None:
            self.payloads.append(payload)

    def __len__(self) -> int:
        return len(self.t)

    def __getitem__(self, i: int):
        if isinstance(i, slice):
            return [self[j] for j in range(*i.indices(len(self)))]
        n = self.names
        p = self.payloads[i] if self.payloads is not None else Size(self.size[i])
        return (n[self.track[i]], self.group[i], self.obj[i], n[self.mode[i]], self.t[i], p)

    def __iter__(self):
        n, pl = self.names, self.payloads
        for i in range(len(self.t)):
            yield (n[self.track[i]], self.group[i], self.obj[i], n[self.mode[i]], self.t[i],
                   pl[i] if pl is not None else Size(self.size[i]))

    def __getstate__(self):
        return {k: v for k, v in self.__dict__.items() if k != "_ix"}

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._ix = {s: i for i, s in enumerate(self.names)}
