from ....core.config_manager import Config
from ...ext_utils.status_utils import (
    MirrorStatus,
    get_readable_file_size,
    get_readable_time,
)


class GoogleDriveStatus:
    def __init__(self, listener, obj, gid, status):
        self._obj = obj
        self._gid = gid
        self._listener = listener
        self._status = status

    def gid(self):
        return self._gid

    def status(self):
        if self._status == "dl":
            return MirrorStatus.STATUS_DOWNLOAD
        else:
            return MirrorStatus.STATUS_UPLOAD

    def name(self):
        return self._listener.name

    def size(self):
        return get_readable_file_size(self._listener.size)

    def processed_bytes(self):
        return get_readable_file_size(self._obj.processed_bytes)

    def progress(self):
        try:
            return f"{round(100 * (self._obj.processed_bytes / self._listener.size), 2)}%"
        except Exception:
            return "0%"

    def speed(self):
        return f"{get_readable_file_size(self._obj.speed)}/s"

    def eta(self):
        try:
            seconds = (self._listener.size - self._obj.processed_bytes) / self._obj.speed
            return get_readable_time(seconds)
        except Exception:
            return "-"

    def task(self):
        return self

    async def cancel_task(self):
        await self._obj.cancel_task()        
