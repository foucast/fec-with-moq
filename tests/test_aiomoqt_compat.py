"""Our code reaches into aiomoqt's fan-out from the outside: the catch-up replay offers items to a
peer's lane, and two deliveries override SubgroupDelivery.write. Neither goes through a public API,
so an aiomoqt release can change them without an error until a live run (0.13.2 added a seventh
lane-item field and an `encoded` keyword; --repair-eog then sent no repair at all). These checks
fail at test time instead."""
import asyncio
import inspect

from aiomoqt.delivery import FanoutDelivery, SubgroupDelivery

import fecmoq.moq as fmoq


class _Lane:
    def __init__(self):
        self.items = []

    def offer(self, item):
        self.items.append(item)


def test_lane_item_has_aiomoqts_shape():
    fan = FanoutDelivery()
    lane = _Lane()
    fan.lanes.append(lane)
    asyncio.run(fan.write(3, 4, b"x", extensions=None, group_start=True))
    theirs = lane.items[0]
    ours = fmoq.lane_item(3, 4, b"x", None, True, 0.0)
    assert len(ours) == len(theirs)
    # group, cost, then object, payload, extensions, group start (live and born differ:
    # a replay is not live, and an untimed fan-out stamps no birth time)
    assert ours[:2] == theirs[:2]
    assert ours[4:8] == theirs[4:8]


def test_lane_queue_and_idle():
    """_drained polls SendLane.items, _source_sent SendLane.idle."""
    from aiomoqt.delivery import SendLane
    assert isinstance(SendLane.idle, property)
    assert "self.items" in inspect.getsource(SendLane.__init__)


def test_swap_delivery_matches_add_delivery():
    """_swap_delivery hands FanoutDelivery.add a bool gate and a float/None deadline, as
    aiomoqt's own _add_delivery does; joining lanes start above the last group."""
    want = set(inspect.signature(FanoutDelivery.add).parameters)
    assert {"gate", "deadline", "joining"} <= want
    src = inspect.getsource(fmoq._swap_delivery)
    assert "lambda" not in src


def test_delivery_overrides_take_every_keyword():
    want = {n for n, p in inspect.signature(SubgroupDelivery.write).parameters.items()
            if p.kind is p.KEYWORD_ONLY}
    for cls in (fmoq.EogDatagramDelivery, fmoq.SubgroupByKeyDelivery):
        have = {n for n, p in inspect.signature(cls.write).parameters.items()
                if p.kind is p.KEYWORD_ONLY}
        assert want <= have, (cls.__name__, want - have)


def test_unsubscribe_reaches_what_it_stops():
    """relay_fec.py pub's unsubscribed() turns a peer's delivery off with the subscription's
    generating flag (a setter that gates its send lane) and detaches it with
    FanoutDelivery.drop_session, which resets the group open for it."""
    from aiomoqt.track import PublishedTrack, _Subscription
    assert isinstance(_Subscription.generating, property) and _Subscription.generating.fset
    assert callable(PublishedTrack._sub_for)
    assert "_out" in inspect.getsource(PublishedTrack._add_delivery)
    src = inspect.getsource(FanoutDelivery.drop_session)
    assert "reset_group" in src and "self.lanes" in src
