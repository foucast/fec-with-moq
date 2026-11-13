"""aiomoqt's LOC media publisher (aiomoqt.tools.pub_media) with the video track on one stream per
Object instead of one per group: the deadline-bounded retransmission regime ("2a").
A relay honouring DELIVERY_TIMEOUT then resets only the late Object's stream, and no later write
of the group meets a reset stream.

Each group still ends with an END_OF_GROUP status Object, on a stream of its own, sent when the next
group starts (aiomoqt's per-Object delivery sends none), so the player knows where a group ends
rather than guessing from the next key frame.

aiomoqt itself is unchanged: its LocTrackPublisher is given the mapping, and its delivery the
end-of-group stream, here, before pub_media builds its tracks.

    python scripts/pub_media_mapped.py <pub_media arguments>
"""
import inspect
import sys

from aiomoqt.delivery import StreamMapping, SubgroupDelivery
from aiomoqt.media import loc
from aiomoqt.messages import SubgroupHeader

_init = loc.LocTrackPublisher.__init__


def _per_object_video(self, *a, **kw):
    _init(self, *a, **kw)
    if self.media_kind == "video":
        self.mapping = StreamMapping.PER_OBJECT


loc.LocTrackPublisher.__init__ = _per_object_video

_write = SubgroupDelivery.write


async def _write_with_end_of_group(self, group_id, object_id, payload, *, extensions=None,
                                   group_start=False):
    last = getattr(self, "_obj_last", None)
    if self.mapping is StreamMapping.PER_OBJECT and last is not None and group_id != last[0]:
        # the previous group is over: its END_OF_GROUP status, one past its last Object
        sid = await self.session.open_uni_stream()
        self.stream_count += 1
        hdr = SubgroupHeader(track_alias=self.track_alias, group_id=last[0], subgroup_id=last[1] + 1,
                             publisher_priority=self.priority, extensions_present=True, prof=self._prof)
        self.session.stream_write(sid, hdr.serialize().data)
        self.session.stream_write(sid, hdr.end_group(object_id=last[1] + 1).data, end_stream=True)
    await _write(self, group_id, object_id, payload, extensions=extensions, group_start=group_start)
    self._obj_last = (group_id, object_id)


SubgroupDelivery.write = _write_with_end_of_group

from aiomoqt.tools import pub_media  # noqa: E402

if __name__ == "__main__":
    sys.argv[0] = "pub_media"
    # Bare LOC payloads, as our FEC publisher sends: aiomoqt >= 0.13 otherwise opens each one
    # with a private-properties block (0x0A 0x00) the browser players take for media. Older
    # pub_media has no --loc-bare and already sends bare LOC.
    if "--loc-bare" in inspect.getsource(pub_media.parse_args) and "--loc-bare" not in sys.argv:
        sys.argv.insert(1, "--loc-bare")
    pub_media.main()
