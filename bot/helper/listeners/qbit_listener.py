from aiofiles.os import remove, path as aiopath
from asyncio import sleep, TimeoutError
from time import time
import time as time_module
import traceback
from aiohttp.client_exceptions import ClientError
from aioqbt.exc import AQError

from ... import (
    task_dict,
    task_dict_lock,
    intervals,
    qb_torrents,
    qb_listener_lock,
    LOGGER,
)
from ...core.config_manager import Config
from ...core.torrent_manager import TorrentManager
from ..ext_utils.bot_utils import new_task
from ..ext_utils.files_utils import clean_unwanted
from ..ext_utils.status_utils import get_readable_time, get_task_by_gid, get_readable_file_size
from ..ext_utils.task_manager import stop_duplicate_check, limit_checker
from ..mirror_leech_utils.status_utils.qbit_status import QbittorrentStatus
from ..telegram_helper.message_utils import update_status_message


def _get_torrent_tag(tor):
    """Safely extract torrent tag with validation.
    
    Args:
        tor: Torrent info object
        
    Returns:
        str: Tag string, or generated fallback if no tags exist
    """
    if not tor.tags or len(tor.tags) == 0:
        LOGGER.warning(f"Torrent {tor.name} has no tags, using hash as fallback")
        return f"tag_{tor.hash[:12]}"
    return tor.tags[0]


async def _remove_torrent(hash_, tag):
    # Bug #31 Fix: Acquire lock BEFORE deleting torrent to prevent race condition
    # The listener loop might try to access the torrent after it's deleted
    async with qb_listener_lock:
        if tag in qb_torrents:
            # Clean up all per-file tracking data to prevent memory leak
            tag_data = qb_torrents[tag]
            
            # Bug #2 Fix: Create list copy to avoid modification during iteration
            file_keys = [k for k in list(tag_data.keys()) if k.startswith(('file_', 'zero_seeders_', 'last_reannounce'))]
            for key in file_keys:
                del tag_data[key]
            
            # Remove the tag entirely
            del qb_torrents[tag]
            
            LOGGER.info(f"Cleaned up {len(file_keys)} tracking keys for {tag}")
    
    # Now safe to delete from qBittorrent - no other code will try to access it
    await TorrentManager.qbittorrent.torrents.delete([hash_], True)
    await TorrentManager.qbittorrent.torrents.delete_tags([tag])


@new_task
async def _on_download_error(err, tor, button=None, is_limit=False):
    LOGGER.info(f"Cancelling Download: {tor.name}")
    ext_hash = tor.hash
    if task := await get_task_by_gid(ext_hash[:12]):
        await task.listener.on_download_error(err, button, is_limit)
    await TorrentManager.qbittorrent.torrents.stop([ext_hash])
    await sleep(0.3)
    await _remove_torrent(ext_hash, _get_torrent_tag(tor))


@new_task
async def _on_seed_finish(tor):
    ext_hash = tor.hash
    LOGGER.info(f"Cancelling Seed: {tor.name}")
    if task := await get_task_by_gid(ext_hash[:12]):
        # Bug #19 Fix: Safe handling of seeding_time which can be None
        seeding_seconds = 0
        if tor.seeding_time is not None:
            seeding_seconds = int(tor.seeding_time.total_seconds())
        msg = f"Seeding stopped with Ratio: {round(tor.ratio, 3)} and Time: {get_readable_time(seeding_seconds)}"
        await task.listener.on_upload_error(msg)
    await _remove_torrent(ext_hash, _get_torrent_tag(tor))


@new_task
async def _stop_duplicate(tor):
    if task := await get_task_by_gid(tor.hash[:12]):
        if task.listener.stop_duplicate:
            # Bug #34 Fix: Safe path parsing to prevent IndexError
            try:
                task.listener.name = tor.content_path.rsplit("/", 1)[-1].rsplit(".!qB", 1)[0]
            except (IndexError, AttributeError):
                # Fallback to torrent name if path parsing fails
                task.listener.name = tor.name
            msg, button = await stop_duplicate_check(task.listener)
            if msg:
                await _on_download_error(msg, tor, button)


@new_task
async def _check_streaming_completion(task, tor_info, tag_data, tag):
    """Check if all selected files are uploaded and complete the streaming task if so.
    
    This function consolidates duplicate completion check logic that was previously
    repeated in two places (lines 296-347 and 408-483).
    
    Args:
        task: The task object containing listener and streaming stats
        tor_info: Torrent info object from qBittorrent
        tag_data: Dictionary containing torrent state (passed to avoid race condition)
        tag: Torrent tag identifier
        
    Returns:
        bool: True if task was completed, False otherwise
    """
    stats = await task.listener._get_streaming_stats(tor_info.hash)
    
    # Fallback: Ensure priority_order exists (Critical for completion check)
    if not stats.get('priority_order') and stats.get('files'):
        LOGGER.warning(f"[STREAMING] Priority order missing for {tor_info.name} during check. Auto-populating...")
        from ..ext_utils.bot_utils import natural_sort_key
        # Create default natural sort order of all file indices
        indexed_files = list(enumerate(stats['files']))
        indexed_files.sort(key=lambda x: natural_sort_key(x[1]))
        stats['priority_order'] = [x[0] for x in indexed_files]
    
    if not stats.get('priority_order'):
        return False  # Cannot check completion without priority order
    
    # Use set intersection for O(n) performance instead of list comprehension
    total_selected = len(stats['priority_order'])
    # Bug #1 Fix: Ensure completed_files is always a set with proper type conversion
    completed_files = stats.get('completed_files', set())
    if not isinstance(completed_files, set):
        completed_files = set(completed_files) if completed_files else set()
    completed_set = set(stats['priority_order']) & completed_files
    total_completed = len(completed_set)
    
    if total_completed >= total_selected and not tag_data.get("streaming_task_completed"):
        tag_data["streaming_task_completed"] = True
        LOGGER.info(f"[STREAMING] All {total_selected} selected files uploaded! Completing task...")
        
        # Send completion message
        from ..telegram_helper.message_utils import send_message
        from ..ext_utils.status_utils import get_readable_file_size, get_readable_time
        # Bug #12 Fix: time_module already imported at top
        
        msg = (
            f"<b><i>{task.listener.name}</i></b>\n"
            f"┟ <b>Selected Files</b> → {total_selected}\n"
        )
        
        # Use selected size if available, otherwise fall back to total
        if stats.get('selected_size', 0) > 0:
            msg += f"┠ <b>Total Size</b> → {get_readable_file_size(stats['selected_size'])}\n"
        else:
            msg += f"┠ <b>Total Size</b> → {get_readable_file_size(task.listener.size)}\n"
        
        msg += (
            f"┠ <b>Time Taken</b> → {get_readable_time(time_module.time() - task.listener.message.date.timestamp())}\n"
            f"┠ <b>In Mode</b> → {task.listener.mode[0]}\n"
            f"┠ <b>Out Mode</b> → {task.listener.mode[1]}\n"
            f"┖ <b>Task By</b> → {task.listener.tag}"
        )
        
        await send_message(task.listener.message, msg)
        
        # Cleanup
        async with task_dict_lock:
            if task.listener.mid in task_dict:
                del task_dict[task.listener.mid]
                LOGGER.info(f"[STREAMING] Removed completed task from task_dict")
        
        await TorrentManager.qbittorrent.torrents.stop([tor_info.hash])
        await _remove_torrent(tor_info.hash, tag)
        return True
    
    return False


@new_task
async def _size_check(tor):
    if task := await get_task_by_gid(tor.hash[:12]):
        task.listener.size = tor.size
        mmsg = await limit_checker(task.listener)
        if mmsg:
            await _on_download_error(mmsg, tor, is_limit=True)


@new_task
async def _on_download_complete(tor):
    ext_hash = tor.hash
    tag = _get_torrent_tag(tor)
    if task := await get_task_by_gid(ext_hash[:12]):
        if not task.listener.seed:
            await TorrentManager.qbittorrent.torrents.stop([ext_hash])
        if task.listener.select:
            await clean_unwanted(task.listener.dir)
            path = tor.content_path.rsplit("/", 1)[0]
            res = await TorrentManager.qbittorrent.torrents.files(ext_hash)
            for f in res:
                if f.priority == 0 and await aiopath.exists(f"{path}/{f.name}"):
                    try:
                        await remove(f"{path}/{f.name}")
                    except (FileNotFoundError, PermissionError, OSError) as e:
                        LOGGER.warning(f"Failed to remove file {f.name}: {e}")
        await task.listener.on_download_complete()
        if intervals["stopAll"]:
            return
        if task.listener.seed and not task.listener.is_cancelled:
            async with task_dict_lock:
                if task.listener.mid in task_dict:
                    removed = False
                    task_dict[task.listener.mid] = QbittorrentStatus(
                        task.listener, True
                    )
                else:
                    removed = True
            if removed:
                await _remove_torrent(ext_hash, tag)
                return
            async with qb_listener_lock:
                if tag in qb_torrents:
                    qb_torrents[tag]["seeding"] = True
                else:
                    return
            await update_status_message(task.listener.message.chat.id)
            LOGGER.info(f"Seeding started: {tor.name} - Hash: {ext_hash}")
        else:
            await _remove_torrent(ext_hash, tag)
            await update_status_message(task.listener.message.chat.id)
    else:
        LOGGER.error(f"Task not found in task_dict for {tor.name} - task was likely cancelled or removed")
        await _remove_torrent(ext_hash, tag)



@new_task
async def _qb_listener():
    while True:
        async with qb_listener_lock:
            try:
                torrents = await TorrentManager.qbittorrent.torrents.info()
                if len(torrents) == 0:
                    intervals["qb"] = ""
                    break
                for tor_info in torrents:
                    tag = _get_torrent_tag(tor_info)

                    if tag not in qb_torrents:

                        # CRITICAL FIX: Handle orphaned torrents (exist in qBit but not in tracking dict)
                        # This happens after bot restart, crash, or initialization failure
                        # Without this, torrents become permanently stuck and invisible to the listener
                        if task := await get_task_by_gid(tor_info.hash[:12]):
                            LOGGER.warning(f"Found orphaned torrent: {tor_info.name}. Re-initializing tracking...")
                            # Re-initialize tracking entry
                            qb_torrents[tag] = {
                                "start_time": time(),
                                "stalled_time": time(),
                                "stop_dup_check": True,  # Already started, skip dup check
                                "size_check": True,      # Already started, skip size check
                                "rechecked": False,
                                "uploaded": False,
                                "seeding": False,
                                "completing": False,
                            }
                            # If it's already 100% complete, force complete it immediately
                            if tor_info.progress >= 0.999 or (tor_info.total_size > 0 and tor_info.downloaded >= tor_info.total_size):
                                LOGGER.info(f"Orphaned torrent {tor_info.name} is 100% complete, triggering completion handler")
                                if not qb_torrents[tag].get("completing", False) and not qb_torrents[tag].get("uploaded", False):
                                    qb_torrents[tag]["completing"] = True
                                    await _on_download_complete(tor_info)
                                continue
                            # Otherwise, let it continue through normal monitoring

                        else:
                            # No task found - this torrent doesn't belong to this bot instance

                            continue
                    state = tor_info.state
                    if state == "metaDL":
                        qb_torrents[tag]["stalled_time"] = time()
                        if (
                            Config.TORRENT_TIMEOUT
                            and time() - qb_torrents[tag]["start_time"]
                            >= Config.TORRENT_TIMEOUT
                        ):
                            await _on_download_error("Dead Torrent!", tor_info)
                        else:
                            await TorrentManager.qbittorrent.torrents.reannounce(
                                [tor_info.hash]
                            )
                    elif state == "downloading":
                        qb_torrents[tag]["stalled_time"] = time()
                        if not qb_torrents[tag]["stop_dup_check"]:
                            qb_torrents[tag]["stop_dup_check"] = True
                            await _stop_duplicate(tor_info)
                        if not qb_torrents[tag]["size_check"]:
                            qb_torrents[tag]["size_check"] = True
                            await _size_check(tor_info)
                        
                        # Bug #8 Fix: Use >= 0.999 for reliable float comparison instead of == 1.0
                        if tor_info.progress >= 0.999 or (tor_info.total_size > 0 and tor_info.downloaded >= tor_info.total_size):
                            if not qb_torrents[tag].get("completing", False) and not qb_torrents[tag].get("uploaded", False):
                                qb_torrents[tag]["completing"] = True
                                await _on_download_complete(tor_info)
                            continue
                    
                    # Check streaming files for ALL download and seeding states
                    # This ensures files are monitored and reannounce happens even when seeding
                    if state in ["downloading", "stalledDL", "pausedDL", "queuedDL", "checkingDL", 
                                 "uploading", "stalledUP", "queuedUP", "forcedUP", "checkingUP",
                                 "stoppedDL"]:
                        if task := await get_task_by_gid(tor_info.hash[:12]):
                            if task.listener.is_streaming:
                                # SOLUTION 2: Periodic Reannounce (every 90 seconds)
                                # Maintains tracker connection during long streaming sessions
                                if not qb_torrents[tag].get("last_reannounce"):
                                    qb_torrents[tag]["last_reannounce"] = 0
                                
                                current_time = time()
                                if current_time - qb_torrents[tag]["last_reannounce"] >= 90:
                                    LOGGER.info(f"[STREAMING] Performing periodic reannounce for {tor_info.name}")
                                    try:
                                        await TorrentManager.qbittorrent.torrents.reannounce([tor_info.hash])
                                        qb_torrents[tag]["last_reannounce"] = current_time
                                    except (AQError, ClientError) as e:
                                        # Bug #18 Fix: Specific exception handling instead of broad Exception
                                        LOGGER.error(f"[STREAMING] Periodic reannounce failed: {e}")
                                        # Don't update timestamp so we retry next cycle
                                
                                # SOLUTION 3: Zero Seeders Detection
                                # Reactive fix when 0 seeders detected but swarm has seeders
                                if tor_info.num_seeds == 0:
                                    # Check if this is a genuine 0 seeder situation or tracker disconnect
                                    # We reannounce if we haven't gotten seeders for a while
                                    if not qb_torrents[tag].get("zero_seeders_last_check"):
                                        qb_torrents[tag]["zero_seeders_last_check"] = current_time
                                        qb_torrents[tag]["zero_seeders_reannounce_count"] = 0
                                    
                                    time_since_last_check = current_time - qb_torrents[tag]["zero_seeders_last_check"]
                                    
                                    # If we've had 0 seeders for more than 30 seconds, try reannounce
                                    if time_since_last_check >= 30 and qb_torrents[tag].get("zero_seeders_reannounce_count", 0) < 3:
                                        LOGGER.warning(f"[STREAMING] 0 seeders detected for {tor_info.name} (state: {state}). Force reannouncing...")
                                        try:
                                            await TorrentManager.qbittorrent.torrents.reannounce([tor_info.hash])
                                            qb_torrents[tag]["zero_seeders_last_check"] = current_time
                                            qb_torrents[tag]["zero_seeders_reannounce_count"] = qb_torrents[tag].get("zero_seeders_reannounce_count", 0) + 1
                                            LOGGER.info(f"[STREAMING] Zero seeders reannounce attempt {qb_torrents[tag]['zero_seeders_reannounce_count']}/3")
                                        except (AQError, ClientError) as e:
                                            # Bug #18 Fix: Specific exception handling instead of broad Exception
                                            LOGGER.error(f"[STREAMING] Zero seeders reannounce failed: {e}")
                                            # Don't update counters so we retry next cycle
                                else:
                                    # Seeders found, reset the zero seeders tracker
                                    if qb_torrents[tag].get("zero_seeders_reannounce_count", 0) > 0:
                                        LOGGER.info(f"[STREAMING] Seeders restored for {tor_info.name}: {tor_info.num_seeds} seeders")
                                    qb_torrents[tag]["zero_seeders_last_check"] = current_time
                                    qb_torrents[tag]["zero_seeders_reannounce_count"] = 0
                                
                                files = await TorrentManager.qbittorrent.torrents.files(tor_info.hash)
                                stats = await task.listener._get_streaming_stats(tor_info.hash)
                                
                                # Auto-initialize if stats are empty (file selection skipped)
                                # Bug #27 Fix: Use .get() to prevent KeyError
                                # Bug #33 Fix: Check if user is selecting files (-s mode)
                                if not stats.get('files') and files and not getattr(task.listener, 'select', False):
                                    LOGGER.info(f"[STREAMING] Stats not initialized. Auto-initializing with all {len(files)} files...")
                                    
                                    # Populate file lists
                                    stats['files'] = [f.name for f in files]
                                    stats['file_sizes'] = [f.size for f in files]
                                    
                                    # Set total size if 0
                                    total_size = sum(f.size for f in files)
                                    if task.listener.size == 0:
                                        task.listener.size = total_size
                                        
                                    stats['selected_size'] = total_size
                                    
                                    # Natural sort order
                                    from ..ext_utils.bot_utils import natural_sort_key
                                    idx_name_map = {f.index: f.name for f in files}
                                    all_ids = [f.index for f in files]
                                    all_ids.sort(key=lambda idx: natural_sort_key(idx_name_map.get(idx, "")))
                                    stats['priority_order'] = all_ids
                                    
                                    # Enable sequential download
                                    props = await TorrentManager.qbittorrent.torrents.properties(tor_info.hash)
                                    if not getattr(props, 'sequential_download', False):
                                        await TorrentManager.qbittorrent.torrents.toggle_sequential_download([tor_info.hash])
                                        LOGGER.info(f"[STREAMING] Enabled sequential download (Auto-init)")
                                    
                                    # Prioritize first file - ensure active_downloads exists
                                    if 'active_downloads' not in stats:
                                        stats['active_downloads'] = []
                                    if all_ids:
                                        first = all_ids[0]
                                        await TorrentManager.qbittorrent.torrents.file_prio(hash=tor_info.hash, id=[first], priority=1)
                                        stats['active_downloads'].append(first)
                                        stats['index'] = first
                                        LOGGER.info(f"[STREAMING] Prioritized first file (index {first}) for auto-start")

                                # Bug #27 Fix: Safe access to active_downloads and other stats keys
                                active_downloads = stats.get('active_downloads', [])
                                LOGGER.info(f"[STREAMING] Monitoring {len(active_downloads)} active files for {tor_info.name}")
                                
                                # SOLUTION 4: Auto-Resume Watchdog
                                # Fixes stuck stoppedDL tasks when disk space is sufficient
                                if state == "stoppedDL" and active_downloads:
                                    try:
                                        from psutil import disk_usage
                                        from ... import DOWNLOAD_DIR
                                        from ..ext_utils.bot_utils import sync_to_async
                                        
                                        free_space = (await sync_to_async(disk_usage, DOWNLOAD_DIR)).free
                                        # Use same threshold (4GB) as disk manager to avoid active fighting
                                        if free_space > 4 * 1024**3:
                                            # Check rate-limit for resume attempts (don't spam resume every 30s)
                                            if not qb_torrents[tag].get("last_resume_attempt") or time() - qb_torrents[tag]["last_resume_attempt"] > 30:
                                                LOGGER.info(f"[STREAMING] Auto-resuming stuck task {tor_info.name} (Free space: {get_readable_file_size(free_space)})")
                                                await TorrentManager.qbittorrent.torrents.resume([tor_info.hash])
                                                qb_torrents[tag]["last_resume_attempt"] = time()
                                    except Exception as e:
                                        LOGGER.error(f"[STREAMING] Auto-resume check failed: {e}")
                                
                                # Bug #20 Fix: Access stats modification with proper consideration for thread safety
                                # Note: stats is returned from async function, should be safe within this context
                                # Bug #3 Fix: Check all active downloads, not just streaming_index
                                for file_index in list(active_downloads):
                                    # Ensure progress tracking is initialized for all monitored files
                                    if file_index < len(files):
                                        current_file = files[file_index]
                                        
                                        # Track file download progress - ensure dict exists
                                        if 'current_file_progress' not in stats:
                                            stats['current_file_progress'] = {}
                                        stats['current_file_progress'][file_index] = {
                                            'downloaded': int(current_file.size * current_file.progress),
                                            'total': current_file.size,
                                            'uploading': False,
                                            'file_name': current_file.name.split('/')[-1]
                                        }
                                        
                                        # Log progress for files actively downloading
                                        # Bug #21 Fix: Import moved to module level
                                        if current_file.progress < 0.999:
                                            LOGGER.info(f"[STREAMING] File {file_index} progress: {current_file.progress * 100:.1f}% ({get_readable_file_size(int(current_file.size * current_file.progress))}/{get_readable_file_size(current_file.size)})")
                                        
                                        # Check if file is complete and trigger upload callback
                                        # Bug #27 Fix: Use .get() for all dict accesses
                                        completed_files = stats.get('completed_files', set())
                                        upload_tasks = stats.get('upload_tasks', {})
                                        if (current_file.progress >= 0.999 and 
                                            file_index not in completed_files and
                                            file_index not in upload_tasks and
                                            not qb_torrents[tag].get(f"file_{file_index}_done")):
                                            qb_torrents[tag][f"file_{file_index}_done"] = True
                                            LOGGER.info(f"[STREAMING] File {file_index} ({current_file.name}) reached {current_file.progress * 100:.1f}% completion. Triggering upload callback.")
                                            await task.listener.on_streaming_file_complete(current_file.name, file_index, tor_info.hash)
                                            
                                            # Memory cleanup: Remove completed file from progress tracking
                                            if 'current_file_progress' in stats and file_index in stats['current_file_progress']:
                                                del stats['current_file_progress'][file_index]
                                    else:
                                        # Defensive: If file_index is out of bounds, initialize with zeros
                                        # This prevents 0% display and will be updated when file data is available
                                        if 'current_file_progress' not in stats:
                                            stats['current_file_progress'] = {}
                                        if file_index not in stats.get('current_file_progress', {}):
                                            stats['current_file_progress'][file_index] = {
                                                'downloaded': 0,
                                                'total': 1,  # Use 1 to avoid division by zero, will show as 0%
                                                'uploading': False,
                                                'file_name': f'File {file_index}'
                                            }
                                            LOGGER.warning(f"[STREAMING] File {file_index} not in qBittorrent files list yet (len={len(files)}), initialized placeholder")

                                # Check for streaming task completion (All selected files uploaded)
                                # This ensures tasks complete even if torrent state is not 'complete' (e.g. prioritized files)
                                if await _check_streaming_completion(task, tor_info, qb_torrents[tag], tag):
                                    continue
                                    
                                # LOG DEBUG INFO if not complete (with rate limiting to avoid spam)
                                if not qb_torrents[tag].get("streaming_task_completed"):
                                    # Track last log time to ensure proper 15-second intervals
                                    current_time = time()
                                    last_debug_log = qb_torrents[tag].get("last_debug_log_time", 0)
                                    
                                    if current_time - last_debug_log >= 15:
                                        stats = await task.listener._get_streaming_stats(tor_info.hash)
                                        # Bug #24 Fix: Use .get() to prevent KeyError
                                        completed_files = stats.get('completed_files', set())
                                        missing = [idx for idx in stats.get('priority_order', []) if idx not in completed_files]
                                        LOGGER.info(f"[STREAMING-DEBUG] Task {task.listener.name} waiting for {len(missing)} files: {missing[:5]}...")
                                        qb_torrents[tag]["last_debug_log_time"] = current_time
                    elif state == "stalledDL":
                        qb_torrents[tag]["stalled_time"] = time()
                        if tor_info.progress >= 0.999:
                            if not qb_torrents[tag].get("completing", False) and not qb_torrents[tag].get("uploaded", False):
                                qb_torrents[tag]["completing"] = True
                                await _on_download_complete(tor_info)
                            continue
                        if (
                            not qb_torrents[tag]["rechecked"]
                            and 0.999 < tor_info.progress < 1
                        ):
                            msg = f"Force recheck - Name: {tor_info.name} Hash: "
                            msg += f"{tor_info.hash} Downloaded Bytes: {tor_info.downloaded} "
                            msg += f"Size: {tor_info.size} Total Size: {tor_info.total_size}"
                            LOGGER.warning(msg)
                            await TorrentManager.qbittorrent.torrents.recheck(
                                [tor_info.hash]
                            )
                            qb_torrents[tag]["rechecked"] = True
                    elif state == "missingFiles":
                        await TorrentManager.qbittorrent.torrents.recheck(
                            [tor_info.hash]
                        )
                    elif state == "error":
                        await _on_download_error(
                            "No enough space for this torrent on device", tor_info
                        )
                    # CRITICAL CATCH-ALL: Handle ANY download state at 100% that wasn't caught above
                    # This fixes torrents stuck in queuedDL, pausedDL, checkingDL, allocating, forcedDL etc.
                    # Root cause: These states were completely unhandled, causing infinite hangs
                    elif (
                        (tor_info.progress >= 0.999 or (tor_info.total_size > 0 and tor_info.downloaded >= tor_info.total_size))
                        and not qb_torrents[tag].get("uploaded", False)
                        and not state.endswith("UP")  # Don't interfere with upload states (handled below)
                    ):
                        if not qb_torrents[tag].get("completing", False):
                            LOGGER.info(f"Torrent {tor_info.name} completed in state '{state}'")
                            qb_torrents[tag]["completing"] = True
                            await _on_download_complete(tor_info)
                        continue
                    
                    if (
                        # Bug #29 Fix: Safe handling of completion_on which can be None
                        # Bug #38 Fix: Also accept progress >= 1.0 as completion signal (sometimes timestamp is missing)
                        (
                            (tor_info.completion_on is not None and int(tor_info.completion_on.timestamp()) != -1)
                            or tor_info.progress >= 1
                        )
                        and not qb_torrents[tag].get("uploaded", False)
                        and state
                        in [
                            "queuedUP",
                            "stalledUP",
                            "uploading",
                            "forcedUP",
                            "stoppedUP",
                            "pausedUP",
                        ]
                    ):
                        # Verbose logging for diagnostics

                        # For streaming mode: check files one final time before completion
                        task = await get_task_by_gid(tor_info.hash[:12])
                        if task and task.listener.is_streaming:

                            if not qb_torrents[tag].get("streaming_finish_logged"):
                                LOGGER.info(f"[STREAMING] Torrent entered seeding state. Performing final file completion check...")
                            
                            # Do a final check for any files that completed between monitoring intervals
                            files = await TorrentManager.qbittorrent.torrents.files(tor_info.hash)
                            stats = await task.listener._get_streaming_stats(tor_info.hash)
                            # Bug #27 Fix: Safe access to active_downloads
                            for file_index in list(stats.get('active_downloads', [])):
                                if file_index < len(files):
                                    current_file = files[file_index]
                                    
                                    # Check if file is complete but callback not yet triggered
                                    # Bug #27 Fix: Safe access to stats dictionaries
                                    completed_files = stats.get('completed_files', set())
                                    upload_tasks = stats.get('upload_tasks', {})
                                    if (current_file.progress >= 0.999 and 
                                        file_index not in completed_files and
                                        file_index not in upload_tasks and
                                        not qb_torrents[tag].get(f"file_{file_index}_done")):
                                        qb_torrents[tag][f"file_{file_index}_done"] = True
                                        LOGGER.info(f"[STREAMING] Final check: File {file_index} ({current_file.name}) completed. Triggering upload callback.")
                                        await task.listener.on_streaming_file_complete(current_file.name, file_index, tor_info.hash)
                                        
                                        # Memory cleanup: Remove completed file from progress tracking
                                        if 'current_file_progress' in stats and file_index in stats['current_file_progress']:
                                            del stats['current_file_progress'][file_index]
                            
                            if not qb_torrents[tag].get("streaming_finish_logged"):
                                LOGGER.info(f"[STREAMING] Ignoring torrent completion event for {tor_info.name} (files processed individually)")
                                qb_torrents[tag]["streaming_finish_logged"] = True
                            
                            # CRITICAL: Check if ALL selected files are uploaded - if so, complete the task
                            if await _check_streaming_completion(task, tor_info, qb_torrents[tag], tag):
                                # Bug #35 Fix: After streaming completion, don't continue - allow seeding state check
                                # Set uploaded flag so we don't re-enter this block
                                qb_torrents[tag]["uploaded"] = True
                        else:
                            # Non-streaming mode: standard completion
                            if not qb_torrents[tag].get("completing", False) and not qb_torrents[tag].get("uploaded", False):
                                qb_torrents[tag]["uploaded"] = True
                                qb_torrents[tag]["completing"] = True
                                await _on_download_complete(tor_info)
                    elif (
                        state in ["stoppedUP", "stoppedDL"]
                        and qb_torrents[tag]["seeding"]
                    ):
                        qb_torrents[tag]["seeding"] = False
                        await _on_seed_finish(tor_info)
                        await sleep(0.5)
            except (ClientError, TimeoutError, AQError) as e:
                # Bug #13 Fix: Proper error handling with traceback (already imported at top)
                LOGGER.error(f"qBittorrent listener error: {str(e)}")
                LOGGER.debug(f"Traceback: {traceback.format_exc()}")
                # Continue loop to retry - transient network errors shouldn't stop monitoring
        await sleep(3)


async def on_download_start(tag):
    async with qb_listener_lock:
        qb_torrents[tag] = {
            "start_time": time(),
            "stalled_time": time(),
            "stop_dup_check": False,
            "size_check": False,
            "rechecked": False,
            "uploaded": False,
            "seeding": False,
            "completing": False,
        }
        if not intervals["qb"]:
            intervals["qb"] = await _qb_listener()
