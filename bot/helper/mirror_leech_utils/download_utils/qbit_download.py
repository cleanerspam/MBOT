from aiofiles.os import remove, path as aiopath
from aiofiles import open as aiopen
from asyncio import sleep, TimeoutError
from aioqbt.api import AddFormBuilder
from aioqbt.exc import AQError
from aiohttp.client_exceptions import ClientError

from .... import (
    task_dict,
    task_dict_lock,
    LOGGER,
    qb_torrents,
)
from ....core.config_manager import Config
from ....core.torrent_manager import TorrentManager
from ...ext_utils.bot_utils import bt_selection_buttons
from ...ext_utils.task_manager import check_running_tasks
from ...listeners.qbit_listener import on_download_start
from ...mirror_leech_utils.status_utils.qbit_status import QbittorrentStatus
from ...mirror_leech_utils.status_utils.streaming_status import StreamingStatus
from ...telegram_helper.message_utils import (
    send_message,
    delete_message,
    send_status_message,
)

"""
Only v1 torrents
#from hashlib import sha1
#from base64 import b16encode, b32decode
#from bencoding import bencode, bdecode
#from re import search as re_search
def _get_hash_magnet(mgt: str):
    hash_ = re_search(r'(?<=xt=urn:btih:)[a-zA-Z0-9]+', mgt).group(0)
    if len(hash_) == 32:
        hash_ = b16encode(b32decode(hash_.upper())).decode()
    return hash_

def _get_hash_file(fpath):
    with open(fpath, "rb") as f:
        decodedDict = bdecode(f.read())
        return sha1(bencode(decodedDict[b'info'])).hexdigest()
"""


async def add_qb_torrent(listener, path, ratio, seed_time):
    if Config.DISABLE_TORRENTS:
        await listener.on_download_error("Torrents are disabled in the configuration.")
        return
    try:
        form = AddFormBuilder.with_client(TorrentManager.qbittorrent)
        if await aiopath.exists(listener.link):
            async with aiopen(listener.link, "rb") as f:
                data = await f.read()
                form = form.include_file(data)
        else:
            form = form.include_url(listener.link)
        form = form.savepath(path).tags([f"{listener.mid}"])
        add_to_queue, event = await check_running_tasks(listener)
        if add_to_queue:
            form = form.stopped(add_to_queue)
        if ratio:
            form = form.ratio_limit(ratio)
        if seed_time:
            form = form.seeding_time_limit(int(seed_time))
        try:
            await TorrentManager.qbittorrent.torrents.add(form.build())
        except (ClientError, TimeoutError, Exception, AQError) as e:
            LOGGER.error(
                f"{e}. {listener.mid}. Already added torrent or unsupported link/file type!"
            )
            await listener.on_download_error(
                f"{e}. {listener.mid}. Already added torrent or unsupported link/file type!"
            )
            return
        tor_info = await TorrentManager.qbittorrent.torrents.info(tag=f"{listener.mid}")
        if len(tor_info) == 0:
            while True:
                if add_to_queue and event.is_set():
                    add_to_queue = False
                tor_info = await TorrentManager.qbittorrent.torrents.info(
                    tag=f"{listener.mid}"
                )
                if len(tor_info) > 0:
                    break
                await sleep(1)
        tor_info = tor_info[0]
        listener.name = tor_info.name
        ext_hash = tor_info.hash

        async with task_dict_lock:
            task_dict[listener.mid] = QbittorrentStatus(listener, queued=add_to_queue)
        await on_download_start(f"{listener.mid}")

        if add_to_queue:
            LOGGER.info(f"Added to Queue/Download: {tor_info.name} - Hash: {ext_hash}")
        else:
            LOGGER.info(f"QbitDownload started: {tor_info.name} - Hash: {ext_hash}")

        await listener.on_download_start()

        # Send status immediately for non-select tasks (matching aria2 behavior)
        if not listener.select and listener.multi <= 1:
            await send_status_message(listener.message)

        if Config.BASE_URL:
            if listener.link.startswith("magnet:") and (listener.select or listener.is_streaming):
                metamsg = "Downloading Metadata, wait then you can select files or wait for streaming. Use torrent file to avoid this wait."
                meta = await send_message(listener.message, metamsg)
                while True:
                    tor_info = await TorrentManager.qbittorrent.torrents.info(
                        tag=f"{listener.mid}"
                    )
                    if len(tor_info) == 0:
                        await delete_message(meta)
                        return
                    try:
                        tor_info = tor_info[0]
                        if tor_info.state not in [
                            "metaDL",
                            "checkingResumeData",
                            "stoppedDL",
                        ]:
                            await delete_message(meta)
                            break
                    except Exception:
                        await delete_message(meta)
                        return

            from ...ext_utils.files_utils import check_storage_threshold
            if not await check_storage_threshold(tor_info.size, 0):
                LOGGER.info(f"[STREAMING] Auto-enabling Streaming Mode for {listener.name}")
                LOGGER.info(f"[STREAMING] Torrent size: {tor_info.size} bytes > Available disk space")
                listener.is_streaming = True
                listener.size = tor_info.size  # Set size for progress tracking
                
                # CRITICAL: Streaming must bypass queue to start immediately
                if add_to_queue:
                    LOGGER.info(f"[STREAMING] Bypassing queue system for immediate start")
                    add_to_queue = False
                    event = None  # Clear queue event
                    # Update task dict to mark as not queued
                    async with task_dict_lock:
                        if listener.mid in task_dict:
                            task_dict[listener.mid].queued = False
                
                # Initialize StreamingStatus immediately for UI updates
                async with task_dict_lock:
                    if listener.mid in task_dict:
                        dl_status = task_dict[listener.mid]
                        if not isinstance(dl_status, StreamingStatus):
                             # CRITICAL: Update status to populate _info before wrapping
                             await dl_status.update()
                             task_dict[listener.mid] = StreamingStatus(listener, dl_status)

            ext_hash = tor_info.hash
            if listener.select:
                # Always show selection dialog when -s flag is used, even in streaming mode
                # CRITICAL FIX: Always stop torrent for selection, even if queued
                # Streaming mode must bypass queue to work correctly
                await TorrentManager.qbittorrent.torrents.stop([ext_hash])
                SBUTTONS = bt_selection_buttons(ext_hash)
                msg = "Your download paused. Choose files then press Done Selecting button to start downloading."
                await send_message(listener.message, msg, SBUTTONS)
            # NOTE: Streaming initialization will happen in file_selector.py after file selection
        elif listener.is_streaming and not listener.select:
            LOGGER.info(f"[DEBUG-STREAMING] ===== ENTERING STREAMING INITIALIZATION BLOCK =====")
            LOGGER.info(f"[DEBUG-STREAMING] ext_hash={ext_hash}, listener.name={listener.name}")
            LOGGER.info(f"[DEBUG-STREAMING] listener.mid={listener.mid}, listener.select={listener.select}")
            
            try:
                LOGGER.info(f"[DEBUG-STREAMING] Fetching torrent files...")
                files = await TorrentManager.qbittorrent.torrents.files(ext_hash)
                LOGGER.info(f"[DEBUG-STREAMING] Got {len(files)} files from qBittorrent")
                
                # Initialize stats for this torrent hash
                stats = listener._get_streaming_stats(ext_hash)
                stats['files'] = [f.name for f in files]
                stats['file_sizes'] = [f.size for f in files]
                file_ids = [f.index for f in files]
                LOGGER.info(f"[STREAMING] Streaming mode enabled: {len(files)} files, total size: {tor_info.size} bytes")
                LOGGER.info(f"[STREAMING] File IDs: {file_ids[:10]}...")  # Show first 10
                
                # Set all files to priority 0 (do not download)
                await TorrentManager.qbittorrent.torrents.file_prio(
                    hash=ext_hash, id=file_ids, priority=0
                )
                LOGGER.info(f"[STREAMING] Set all {len(files)} files to priority 0 (do not download)")
                LOGGER.info(f"[DEBUG-STREAMING] Checking file priorities are reset...")
                
                # Initialize priority order with natural sort
                from ...ext_utils.bot_utils import natural_sort_key
                indexed_files = list(enumerate(stats['files']))
                indexed_files.sort(key=lambda x: natural_sort_key(x[1]))
                stats['priority_order'] = [x[0] for x in indexed_files]
                LOGGER.info(f"[DEBUG-STREAMING] Priority Order Calculated: {stats['priority_order'][:5]}...")
                
                # Prioritize the first file from sorted order
                first_file_idx = stats['priority_order'][0]
                LOGGER.info(f"[DEBUG-STREAMING] Setting Priority 1 for First File Index: {first_file_idx}")
                await TorrentManager.qbittorrent.torrents.file_prio(
                    hash=ext_hash, id=[first_file_idx], priority=1
                )
                stats['active_downloads'].append(first_file_idx) 
                stats['index'] = first_file_idx
                LOGGER.info(f"[DEBUG-STREAMING] Updated listener state:")
                LOGGER.info(f"[DEBUG-STREAMING]   streaming_active_downloads = {stats['active_downloads']}")
                LOGGER.info(f"[DEBUG-STREAMING]   streaming_index = {stats['index']}")
                LOGGER.info(f"[STREAMING] Prioritized first alphabetical file (index {first_file_idx}): {files[first_file_idx].name}")
                LOGGER.info(f"[STREAMING] Active downloads list: {stats['active_downloads']}")
                LOGGER.info(f"[STREAMING] Enabling sequential download for torrent")
                
                # Bug #5 Fix: Enable sequential download explicitly
                torrent_properties = await TorrentManager.qbittorrent.torrents.properties(ext_hash)
                if not getattr(torrent_properties, 'sequential_download', False):
                    await TorrentManager.qbittorrent.torrents.toggle_sequential_download([ext_hash])
                    LOGGER.info(f"[STREAMING] Sequential download toggled ON")
                else:
                    LOGGER.info(f"[STREAMING] Sequential download already enabled")
                
                # Start the torrent
                await TorrentManager.qbittorrent.torrents.resume([ext_hash])
                LOGGER.info(f"[STREAMING] Torrent started")
                
                # Verify state
                LOGGER.info(f"[DEBUG-STREAMING] Verifying torrent state after start...")
                info_check = await TorrentManager.qbittorrent.torrents.info(hashes=[ext_hash])
                if info_check:
                    LOGGER.info(f"[DEBUG-STREAMING] Current State: {info_check[0].state}, Dlspeed: {info_check[0].dlspeed}")
                else:
                    LOGGER.info(f"[DEBUG-STREAMING] Could not fetch info after start!")

                await send_status_message(listener.message)
            except Exception as e:
                LOGGER.error(f"[STREAMING] Error during streaming setup: {e}", exc_info=True)
                await listener.on_download_error(f"Failed to initialize streaming mode: {e}")
                return


        elif listener.multi <= 1:
            await send_status_message(listener.message)

        if event is not None:
            if not event.is_set():
                await event.wait()
                if listener.is_cancelled:
                    return
                async with task_dict_lock:
                    task_dict[listener.mid].queued = False
                LOGGER.info(
                    f"Start Queued Download from Qbittorrent: {tor_info.name} - Hash: {ext_hash}"
                )
            await on_download_start(f"{listener.mid}")
            await TorrentManager.qbittorrent.torrents.start([ext_hash])
    except (ClientError, TimeoutError, Exception, AQError) as e:
        if f"{listener.mid}" in qb_torrents:
            del qb_torrents[f"{listener.mid}"]
        await listener.on_download_error(f"{e}")
    finally:
        if await aiopath.exists(listener.link):
            await remove(listener.link)
