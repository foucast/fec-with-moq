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
    ours = fmoq.lane_item(3, 4, b"x", None, True, 0.0)
    assert len(ours) == len(lane.items[0])
    assert ours[:5] == lane.items[0][:5]


def test_delivery_overrides_take_every_keyword():
    want = {n for n, p in inspect.signature(SubgroupDelivery.write).parameters.items()
            if p.kind is p.KEYWORD_ONLY}
    for cls in (fmoq.EogDatagramDelivery, fmoq.SubgroupByKeyDelivery):
        have = {n for n, p in inspect.signature(cls.write).parameters.items()
                if p.kind is p.KEYWORD_ONLY}
        assert want <= have, (cls.__name__, want - have)
