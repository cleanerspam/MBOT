from aiofiles.os import remove, path as aiopath
from asyncio import iscoroutinefunction

from .. import (
    task_dict,
    task_dict_lock,
    user_data,
    LOGGER,
)
from ..core.config_manager import Config
from ..core.torrent_manager import TorrentManager
from ..helper.ext_utils.bot_utils import (
    bt_selection_buttons,
    new_task,
)
from ..helper.ext_utils.status_utils import get_task_by_gid, MirrorStatus
from ..helper.mirror_leech_utils.status_utils.streaming_status import StreamingStatus
from ..helper.telegram_helper.message_utils import (
    send_message,
    send_status_message,
    delete_message,
)


@new_task
async def select(_, message):
    if not Config.BASE_URL:
        await send_message(message, "Base URL not defined!")
        return
    user_id = message.from_user.id
    msg = message.text.split()
    if len(msg) > 1:
        gid = msg[1]
        task = await get_task_by_gid(gid)
        if task is None:
            await send_message(message, f"GID: <code>{gid}</code> Not Found.")
            return
    elif reply_to_id := message.reply_to_message_id:
        async with task_dict_lock:
            task = task_dict.get(reply_to_id)
        if task is None:
            await send_message(message, "This is not an active task!")
            return
    elif len(msg) == 1:
        msg = (
            "Reply to an active /cmd which was used to start the download or add gid along with cmd\n\n"
            + "This command mainly for selection incase you decided to select files from already added torrent. "
            + "But you can always use /cmd with arg `s` to select files before download start."
        )
        await send_message(message, msg)
        return
    if (
        Config.OWNER_ID != user_id
        and task.listener.user_id != user_id
        and (user_id not in user_data or not user_data[user_id].get("SUDO"))
    ):
        await send_message(message, "This task is not for you!")
        return
    if not iscoroutinefunction(task.status):
        await send_message(message, "The task have finshed the download stage!")
        return
    if await task.status() not in [
        MirrorStatus.STATUS_DOWNLOAD,
        MirrorStatus.STATUS_PAUSED,
        MirrorStatus.STATUS_QUEUEDL,
    ]:
        await send_message(
            message,
            "Task should be in download or pause (incase message deleted by wrong) or queued status (incase you have used torrent file)!",
        )
        return
    if task.name().startswith("[METADATA]") or task.name().startswith("Trying"):
        await send_message(message, "Try after downloading metadata finished!")
        return

    try:
        if not task.queued:
            await task.update()
            id_ = task.gid()
            if task.listener.is_qbit:
                id_ = task.hash()
                await TorrentManager.qbittorrent.torrents.stop([id_])
            else:
                try:
                    await TorrentManager.aria2.forcePause(id_)
                except Exception as e:
                    LOGGER.error(
                        f"{e} Error in pause, this mostly happens after abuse aria2"
                    )
        task.listener.select = True
    except Exception:
        await send_message(message, "This is not a bittorrent task!")
        return

    SBUTTONS = bt_selection_buttons(id_)
    msg = "Your download paused. Choose files then press Done Selecting button to resume downloading."
    await send_message(message, msg, SBUTTONS)


@new_task
async def confirm_selection(_, query):
    LOGGER.info(f"[CALLBACK-DEBUG] ===== CONFIRM_SELECTION CALLED =====")
    user_id = query.from_user.id
    data = query.data.split()
    LOGGER.info(f"[CALLBACK-DEBUG] Raw callback data: {query.data}")
    LOGGER.info(f"[CALLBACK-DEBUG] Parsed data: {data}")
    message = query.message
    task = await get_task_by_gid(data[2])
    LOGGER.info(f"[CALLBACK-DEBUG] Task lookup by gid={data[2]}: {'Found' if task else 'NOT FOUND'}")
    if task is None:
        await query.answer("This task has been cancelled!", show_alert=True)
        await delete_message(message)
        return
    if user_id != task.listener.user_id:
        await query.answer("This task is not for you!", show_alert=True)
    elif data[1] == "pin":
        await query.answer(data[3], show_alert=True)
    elif data[1] == "cancel":
        # User explicitly cancelled - clean up
        LOGGER.info(f"[CALLBACK-DEBUG] Cancel button confirmed - cancelling task")
        await query.answer()
        await delete_message(message)
        await task.cancel_task()
    elif data[1] == "done":
        LOGGER.info(f"[CALLBACK-DEBUG] Done button confirmed - processing...")
        await query.answer()
        id_ = data[3]
        
        # CRITICAL FIX: If task is StreamingStatus, unwrap to get actual download status
        actual_task = task
        if isinstance(task, StreamingStatus):
            LOGGER.info(f"[CALLBACK-DEBUG] Task is StreamingStatus - unwrapping to get download_status")
            actual_task = task.download_status
            LOGGER.info(f"[CALLBACK-DEBUG] Unwrapped task type: {type(actual_task).__name__}")
        
        LOGGER.info(f"[CALLBACK-DEBUG] Checking task attributes - hasattr(actual_task, 'seeding'): {hasattr(actual_task, 'seeding')}")
        if hasattr(actual_task, "seeding"):
            LOGGER.info(f"[CALLBACK-DEBUG] Task has 'seeding' attribute")
            LOGGER.info(f"[CALLBACK-DEBUG] actual_task.listener.is_qbit: {actual_task.listener.is_qbit}")
            if actual_task.listener.is_qbit:
                LOGGER.info(f"[CALLBACK-DEBUG] Is qBit task - fetching torrent info...")
                tor_info = (
                    await TorrentManager.qbittorrent.torrents.info(hashes=[id_])
                )[0]
                LOGGER.info(f"[CALLBACK-DEBUG] Got torrent info - content_path: {tor_info.content_path}")
                path = tor_info.content_path.rsplit("/", 1)[0]
                LOGGER.info(f"[CALLBACK-DEBUG] Fetching file list...")
                res = await TorrentManager.qbittorrent.torrents.files(id_)
                LOGGER.info(f"[CALLBACK-DEBUG] Got {len(res)} files - removing unselected files...")
                for f in res:
                    if f.priority == 0:
                        f_paths = [f"{path}/{f.name}", f"{path}/{f.name}.!qB"]
                        for f_path in f_paths:
                            if await aiopath.exists(f_path):
                                try:
                                    await remove(f_path)
                                except Exception:
                                    pass
                LOGGER.info(f"[CALLBACK-DEBUG] Finished removing unselected files")
                LOGGER.info(f"[CALLBACK-DEBUG] Checking streaming - is_streaming: {task.listener.is_streaming}")
                if task.listener.is_streaming:
                    # Complete streaming initialization with user-selected files
                    # This replaces the logic that would have run in qbit_download.py
                    LOGGER.info(f"[STREAMING-DEBUG] ===== ENTERING FILE SELECTION STREAMING INIT =====")
                    LOGGER.info(f"[STREAMING-DEBUG] Hash: {id_}, listener.mid: {task.listener.mid}")
                    LOGGER.info(f"[STREAMING-DEBUG] is_streaming: {task.listener.is_streaming}, select: {task.listener.select}")
                    LOGGER.info(f"[STREAMING] Initializing streaming with selected files for {id_}")
                    
                    # Upgrade status to StreamingStatus if not already
                    async with task_dict_lock:
                        if task.listener.mid in task_dict:
                            dl_status = task_dict[task.listener.mid]
                            LOGGER.info(f"[STREAMING-DEBUG] Current status type: {type(dl_status).__name__}")
                            if not isinstance(dl_status, StreamingStatus):
                                await dl_status.update()
                                task_dict[task.listener.mid] = StreamingStatus(task.listener, dl_status)
                                task = task_dict[task.listener.mid]
                                LOGGER.info(f"[STREAMING-DEBUG] Upgraded to StreamingStatus")
                    
                    # Get file list from qBittorrent
                    try:
                        LOGGER.info(f"[STREAMING-DEBUG] Fetching files from qBittorrent...")
                        files = await TorrentManager.qbittorrent.torrents.files(id_)
                        LOGGER.info(f"[STREAMING-DEBUG] Got {len(files)} files")
                        
                        # Initialize stats for this torrent hash  
                        stats = await task.listener._get_streaming_stats(id_)
                        stats['files'] = [f.name for f in files]
                        stats['file_sizes'] = [f.size for f in files]
                        LOGGER.info(f"[STREAMING] Got {len(files)} total files")
                        
                        # Identify selected vs unselected based on priority
                        unselected_ids = [f.index for f in res if f.priority == 0]
                        selected_ids = [f.index for f in res if f.priority > 0]
                        
                        LOGGER.info(f"[STREAMING] Selected: {len(selected_ids)}, Unselected: {len(unselected_ids)}")
                        LOGGER.info(f"[STREAMING-DEBUG] Selected IDs: {selected_ids[:10]}")
                        
                        # CRITICAL: Set total torrent size first (fixes "Total Size: 0B" bug)
                        total_size = sum(f.size for f in files)
                        if task.listener.size == 0 or task.listener.size != total_size:
                            task.listener.size = total_size
                            LOGGER.info(f"[STREAMING] Set total torrent size: {total_size / (1024**3):.2f} GB")
                        
                        # CRITICAL: Calculate total size of SELECTED files only for accurate progress
                        selected_size = sum(files[idx].size for idx in selected_ids)
                        stats['selected_size'] = selected_size
                        LOGGER.info(f"[STREAMING] Selected files total size: {selected_size / (1024**3):.2f} GB out of {task.listener.size / (1024**3):.2f} GB")
                        
                        # Mark unselected as completed so they're skipped
                        stats['completed_files'].update(unselected_ids)
                        
                        # Natural sort selected files by name for processing order
                        from ..helper.ext_utils.bot_utils import natural_sort_key
                        idx_name_map = {f.index: f.name for f in files}
                        selected_ids.sort(key=lambda idx: natural_sort_key(idx_name_map.get(idx, "")))
                        stats['priority_order'] = selected_ids
                        
                        LOGGER.info(f"[STREAMING] Priority order (first 5): {selected_ids[:5]}")
                        
                        # Reset ALL files to priority 0 first (so we have full control)
                        all_ids = [f.index for f in files]
                        LOGGER.info(f"[STREAMING-DEBUG] Resetting all {len(all_ids)} files to priority 0...")
                        await TorrentManager.qbittorrent.torrents.file_prio(hash=id_, id=all_ids, priority=0)
                        LOGGER.info(f"[STREAMING] Reset all files to priority 0")
                        
                        # Start with first selected file
                        if selected_ids:
                            first = selected_ids[0]
                            LOGGER.info(f"[STREAMING-DEBUG] Setting first file (index {first}) to priority 1...")
                            await TorrentManager.qbittorrent.torrents.file_prio(hash=id_, id=[first], priority=1)
                            stats['active_downloads'].append(first)
                            stats['index'] = first
                            LOGGER.info(f"[STREAMING] Prioritized first file (index {first}): {idx_name_map[first]}")
                        
                        # Enable sequential download
                        LOGGER.info(f"[STREAMING-DEBUG] Checking sequential download...")
                        props = await TorrentManager.qbittorrent.torrents.properties(id_)
                        if not getattr(props, 'sequential_download', False):
                            await TorrentManager.qbittorrent.torrents.toggle_sequential_download([id_])
                            LOGGER.info(f"[STREAMING] Enabled sequential download")
                        else:
                            LOGGER.info(f"[STREAMING-DEBUG] Sequential already enabled")
                        
                        # CRITICAL: Start the torrent
                        LOGGER.info(f"[STREAMING-DEBUG] ===== ATTEMPTING TO START TORRENT =====")
                        LOGGER.info(f"[STREAMING-DEBUG] Calling resume([{id_}])...")
                        await TorrentManager.qbittorrent.torrents.resume([id_])
                        LOGGER.info(f"[STREAMING-DEBUG] Resume called successfully")
                        
                        LOGGER.info(f"[STREAMING-DEBUG] Calling set_force_start([{id_}], True)...")
                        await TorrentManager.qbittorrent.torrents.set_force_start([id_], True)
                        LOGGER.info(f"[STREAMING] Torrent force-started")
                        
                        # Verify it's actually running
                        import asyncio
                        LOGGER.info(f"[STREAMING-DEBUG] Waiting 2 seconds before verification...")
                        await asyncio.sleep(2)
                        
                        LOGGER.info(f"[STREAMING-DEBUG] Verifying torrent state...")
                        verify_info = await TorrentManager.qbittorrent.torrents.info(hashes=[id_])
                        if verify_info:
                            state = verify_info[0].state
                            dlspeed = verify_info[0].dlspeed
                            seeders = verify_info[0].num_seeds
                            leechers = verify_info[0].num_leechs
                            LOGGER.info(f"[STREAMING] Verified state: {state}, speed: {dlspeed}, seeders: {seeders}, leechers: {leechers}")
                            
                            # If still stuck, try aggressive restart
                            if dlspeed == 0 and state in ['pausedDL', 'stoppedDL', 'stalledDL']:
                                LOGGER.warning(f"[STREAMING-DEBUG] Torrent stalled! State: {state}")
                                LOGGER.warning(f"[STREAMING] Torrent stalled! Attempting reannounce...")
                                await TorrentManager.qbittorrent.torrents.reannounce([id_])
                                await asyncio.sleep(1)
                                await TorrentManager.qbittorrent.torrents.resume([id_])
                                await TorrentManager.qbittorrent.torrents.set_force_start([id_], True)
                                
                                # Check again
                                await asyncio.sleep(2)
                                verify_info2 = await TorrentManager.qbittorrent.torrents.info(hashes=[id_])
                                if verify_info2:
                                    LOGGER.info(f"[STREAMING-DEBUG] After reannounce - state: {verify_info2[0].state}, speed: {verify_info2[0].dlspeed}")
                        else:
                            LOGGER.error(f"[STREAMING-DEBUG] Could not fetch torrent info!")
                                
                    except Exception as e:
                        LOGGER.error(f"[STREAMING] Failed to initialize: {e}", exc_info=True)
                
                # Start the torrent if not queued and not streaming
                if not task.listener.is_streaming and not task.queued:
                    await TorrentManager.qbittorrent.torrents.start([id_])
            else:
                res = await TorrentManager.aria2.getFiles(id_)
                for f in res:
                    if f["selected"] == "false" and await aiopath.exists(f["path"]):
                        try:
                            await remove(f["path"])
                        except Exception:
                            pass
                if not task.queued:
                    try:
                        await TorrentManager.aria2.unpause(id_)
                    except Exception as e:
                        LOGGER.error(
                            f"{e} Error in resume, this mostly happens after abuse aria2. Try to use select cmd again!"
                        )
        await send_status_message(message)
        await delete_message(message)
