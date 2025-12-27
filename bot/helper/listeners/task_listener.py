from asyncio import gather, sleep, create_task as asyncio_create_task
from html import escape
from time import time
from mimetypes import guess_type
from contextlib import suppress
from os import path as ospath

from aiofiles.os import listdir, remove, path as aiopath
from requests import utils as rutils

from ... import (
    intervals,
    task_dict,
    task_dict_lock,
    LOGGER,
    non_queued_up,
    non_queued_dl,
    queued_up,
    queued_dl,
    queue_dict_lock,
    same_directory_lock,
    DOWNLOAD_DIR,
)
from ...modules.metadata import apply_metadata_title
from ..common import TaskConfig
from ...core.tg_client import TgClient
from ...core.config_manager import Config
from ...core.torrent_manager import TorrentManager
from ..ext_utils.bot_utils import sync_to_async
from ..ext_utils.links_utils import encode_slink
from ..ext_utils.db_handler import database
from ..ext_utils.files_utils import (
    clean_download,
    clean_target,
    create_recursive_symlink,
    get_path_size,
    join_files,
    remove_excluded_files,
    move_and_merge,
)
from ..ext_utils.links_utils import is_gdrive_id
from ..ext_utils.status_utils import get_readable_file_size, get_readable_time
from ..ext_utils.task_manager import check_running_tasks, start_from_queued
# from ..mirror_leech_utils.gdrive_utils.upload import GoogleDriveUpload  # Module removed
from ..mirror_leech_utils.rclone_utils.transfer import RcloneTransferHelper
from ..mirror_leech_utils.status_utils.gdrive_status import (
    GoogleDriveStatus,
)
from ..mirror_leech_utils.status_utils.queue_status import QueueStatus
from ..mirror_leech_utils.status_utils.streaming_status import StreamingStatus
from ..mirror_leech_utils.status_utils.telegram_status import TelegramStatus
from ..mirror_leech_utils.status_utils.yt_status import YtStatus
from ..mirror_leech_utils.upload_utils.telegram_uploader import TelegramUploader
from ..mirror_leech_utils.youtube_utils.youtube_upload import YouTubeUpload
from ..telegram_helper.button_build import ButtonMaker
from ..telegram_helper.message_utils import (
    delete_message,
    delete_status,
    send_message,
    update_status_message,
)

# Streaming Configuration Constants
STREAMING_MAX_CONCURRENT_UPLOADS = 3  # Max simultaneous file uploads
STREAMING_MAX_CONCURRENT_DOWNLOADS = 5  # Max parallel file downloads
STREAMING_UPLOAD_TIMEOUT_SECONDS = 1800  # 30 minutes per upload attempt
STREAMING_MAX_UPLOAD_RETRIES = 4  # Total upload attempts per file
STREAMING_DISK_CHECK_INTERVAL = 60  # Seconds between disk space checks
STREAMING_MIN_FREE_SPACE_GB = 2  # Minimum free disk space in GB
STREAMING_SPEED_SAMPLE_WINDOW = 10  # Number of speed samples to keep
STREAMING_COMPLETION_WAIT_TIMEOUT = 3600  # 1 hour max wait for file completion


class TaskListener(TaskConfig):
    def __init__(self):
        super().__init__()
        # Initialize streaming stats lock for thread safety
        from asyncio import Lock
        self.streaming_stats_lock = Lock()

    async def _get_streaming_stats(self, torrent_hash):
        """Get streaming stats for a specific torrent hash. Creates if doesn't exist.
        
        CRITICAL: This method is async and uses a lock to prevent race conditions
        where multiple tasks could create duplicate stat dictionaries.
        """
        async with self.streaming_stats_lock:
            if torrent_hash not in self.streaming_stats:
                from asyncio import Semaphore, Lock
                self.streaming_stats[torrent_hash] = {
                    'index': 0,
                    'files': [],
                    'file_sizes': [],
                    'active_downloads': [],
                    'speeds': {'download': [], 'upload': [], 'last_check': 0},
                    'upload_queue': [],
                    'upload_tasks': {},
                    'completed_files': set(),
                    'upload_semaphore': Semaphore(STREAMING_MAX_CONCURRENT_UPLOADS),
                    'index_lock': Lock(),
                    'current_file_progress': {},
                    'uploaders': {},
                    'total_uploaded_bytes': 0,
                    'priority_order': [],
                    'ready_to_upload': set(),
                    'selected_size': 0,
                    'file_path_map': {},  # Cache for file paths
                }
                LOGGER.info(f"[STREAMING] Initialized stats for torrent {torrent_hash[:12]}")
            return self.streaming_stats[torrent_hash]
    
    async def _cleanup_streaming_stats(self, torrent_hash):
        """Clean up streaming stats for a completed/cancelled torrent."""
        async with self.streaming_stats_lock:
            if torrent_hash in self.streaming_stats:
                del self.streaming_stats[torrent_hash]
                LOGGER.info(f"[STREAMING] Cleaned up stats for torrent {torrent_hash[:12]}")


    async def clean(self):
        with suppress(Exception):
            if st := intervals["status"]:
                for intvl in list(st.values()):
                    intvl.cancel()
            intervals["status"].clear()
            await gather(TorrentManager.aria2.purgeDownloadResult(), delete_status())

    def clear(self):
        self.subname = ""
        self.subsize = 0
        self.files_to_proceed = []
        self.proceed_count = 0
        self.progress = True

    async def remove_from_same_dir(self):
        async with task_dict_lock:
            if (
                self.folder_name
                and self.same_dir
                and self.mid in self.same_dir[self.folder_name]["tasks"]
            ):
                self.same_dir[self.folder_name]["tasks"].remove(self.mid)
                self.same_dir[self.folder_name]["total"] -= 1

    async def on_download_start(self):
        mode_name = "Leech" if self.is_leech else "Mirror"
        if self.bot_pm and self.is_super_chat:
            self.pm_msg = await send_message(
                self.user_id,
                f"""➲ <b><u>Task Started :</u></b>
┃
┖ <b>Link:</b> <a href='{self.source_url}'>Click Here</a>
""",
            )
        if Config.LINKS_LOG_ID:
            await send_message(
                Config.LINKS_LOG_ID,
                f"""➲  <b><u>{mode_name} Started:</u></b>
 ┃
 ┠ <b>User :</b> {self.tag} ( #ID{self.user_id} )
 ┠ <b>Message Link :</b> <a href='{self.message.link}'>Click Here</a>
 ┗ <b>Link:</b> <a href='{self.source_url}'>Click Here</a>
 """,
            )
        if (
            self.is_super_chat
            and Config.INCOMPLETE_TASK_NOTIFIER
            and Config.DATABASE_URL
        ):
            await database.add_incomplete_task(
                self.message.chat.id, self.message.link, self.tag
            )

        # Initialize StreamingStatus immediately if this is a streaming task
        if self.is_streaming:
            async with task_dict_lock:
                if self.mid in task_dict:
                    dl_status = task_dict[self.mid]
                    # Only wrap if not already wrapped
                    if not isinstance(dl_status, StreamingStatus):
                        # Force update to populate _info
                        await dl_status.update()
                        task_dict[self.mid] = StreamingStatus(self, dl_status)
                        LOGGER.info(f"Initialized StreamingStatus for task {self.mid}")

    async def on_streaming_file_complete(self, file_name, file_index, torrent_hash):
        """Handle completion of a downloaded file in streaming mode.
        
        Args:
            file_name: Name of the completed file (may include subdirectories)
            file_index: Index of the file in the torrent
            torrent_hash: Hash of the torrent (for stats isolation)
        """
        try:
            # Get stats for this specific torrent
            stats = await self._get_streaming_stats(torrent_hash)
            
            # Bug #3 FIX: File path construction with caching
            # Check cache first to avoid expensive os.walk
            cache_key = f"{file_index}_{file_name}"
            
            if cache_key in stats.get('file_path_map', {}):
                path = stats['file_path_map'][cache_key]
                LOGGER.debug(f"[STREAMING] Using cached path for file {file_index}: {path}")
            else:
                # Try direct path first, then with .!qB extension
                path = f"{self.dir}/{file_name}"
                if not await aiopath.exists(path):
                    path = f"{self.dir}/{file_name}.!qB"
                    if not await aiopath.exists(path):
                        # Search in subdirectories (slow path - only on cache miss)
                        from os import path as ospath
                        base_name = ospath.basename(file_name)
                        try:
                            for root, dirs, files in await sync_to_async(lambda: list(__import__('os').walk(self.dir))):
                                if base_name in files:
                                    path = ospath.join(root, base_name)
                                    break
                                elif f"{base_name}.!qB" in files:
                                    path = ospath.join(root, f"{base_name}.!qB")
                                    break
                        except Exception as e:
                            LOGGER.error(f"Error searching for file: {e}")
                        
                        if not await aiopath.exists(path):
                            LOGGER.error(f"Streaming file not found after search: {file_name}")
                            return
                
                # Cache the resolved path for future use
                if 'file_path_map' not in stats:
                    stats['file_path_map'] = {}
                stats['file_path_map'][cache_key] = path
                LOGGER.debug(f"[STREAMING] Cached path for file {file_index}: {path}")

            LOGGER.info(f"[STREAMING] File {file_index} ({file_name}) download complete. Queueing for upload.")
            
            # Queue for upload instead of starting immediately
            stats['ready_to_upload'].add(file_index)
            
            # Bug #6: Check for cancellation
            if self.is_cancelled:
                LOGGER.info(f"[STREAMING] Task cancelled. Performing cleanup...")
                await self._streaming_cleanup(torrent_hash)
                return
            
            # Update status to StreamingStatus if needed
            async with task_dict_lock:
                if self.mid in task_dict:
                    dl_status = task_dict[self.mid]
                    if not isinstance(dl_status, StreamingStatus):
                        task_dict[self.mid] = StreamingStatus(self, dl_status)
            
            # Manage uploads (start next in priority order)
            await self._manage_uploads(torrent_hash)
            
            # Optimization: Remove from active downloads immediately
            if file_index in stats['active_downloads']:
                stats['active_downloads'].remove(file_index)
            
            # ALWAYS run adaptive logic to prioritize next files
            LOGGER.info(f"[STREAMING] Running adaptive bandwidth management...")
            await self._manage_adaptive_downloads(torrent_hash)
            
            # Bug #8: Check and manage disk space
            await self._check_and_manage_disk_space(torrent_hash)
            
        except Exception as e:
            LOGGER.error(f"Critical error in on_streaming_file_complete for file {file_index} ({file_name}): {e}", exc_info=True)
            # Don't propagate - just log and continue with other files
            # Remove from active downloads to allow next file to be prioritized
            stats = await self._get_streaming_stats(torrent_hash)
            if file_index in stats['active_downloads']:
                stats['active_downloads'].remove(file_index)


    
    async def _upload_file_with_retry(self, file_index, file_name, file_path, torrent_hash):
        """Upload a single file with retry logic and proper cleanup."""
        stats = await self._get_streaming_stats(torrent_hash)
        async with stats['upload_semaphore']:
            LOGGER.info(f"Starting upload for file {file_index} ({file_name})")
            
            try:
                # Verify file exists before upload
                if not await aiopath.exists(file_path):
                    # Try with .!qB extension (qBittorrent temp file)
                    if await aiopath.exists(f"{file_path}.!qB"):
                        file_path = f"{file_path}.!qB"
                    else:
                        LOGGER.error(f"File not found for upload: {file_path}")
                        return False
                
                # Mark file as uploading in progress tracker
                from os import path as ospath
                file_size = await get_path_size(file_path) if await aiopath.exists(file_path) else 0
                stats['current_file_progress'][file_index] = {
                    'downloaded': file_size,
                    'total': file_size,
                    'uploading': True,
                    'file_name': ospath.basename(file_name)
                }
                
                # CRITICAL FIX: Pass file directly, not parent directory
                # TelegramUploader uses os.walk, so passing a file will upload only that file
                # Passing a directory would upload ALL files in that directory
                uploader = TelegramUploader(self, file_path)
                # Store uploader for speed monitoring
                stats['uploaders'][file_index] = uploader
                
                # Upload with retry + timeout + exponential backoff
                upload_success = False
                from asyncio import wait_for, TimeoutError
                
                for attempt in range(STREAMING_MAX_UPLOAD_RETRIES):
                    try:
                        LOGGER.info(f"Upload attempt {attempt + 1}/{STREAMING_MAX_UPLOAD_RETRIES} for file {file_index} ({file_name})")
                        
                        # Timeout per attempt
                        await wait_for(uploader.upload(), timeout=STREAMING_UPLOAD_TIMEOUT_SECONDS)
                        
                        upload_success = True
                        LOGGER.info(f"Upload successful for file {file_index} ({file_name})")
                        break
                        
                    except TimeoutError:
                        LOGGER.error(f"Upload timeout (attempt {attempt + 1}/{STREAMING_MAX_UPLOAD_RETRIES}) for file {file_index} ({file_name})")
                        if attempt < STREAMING_MAX_UPLOAD_RETRIES - 1:
                            # Exponential backoff: 5s, 10s, 20s
                            delay = 5 * (2 ** attempt)
                            LOGGER.info(f"Retrying in {delay} seconds...")
                            await sleep(delay)
                        else:
                            LOGGER.error(f"All upload attempts timed out for file {file_index} ({file_name}). Skipping file.")
                            
                    except FileNotFoundError as e:
                        LOGGER.error(f"File not found, cannot retry: {e}")
                        break  # Don't retry unrecoverable errors
                    except Exception as e:
                        LOGGER.error(f"Upload attempt {attempt + 1}/{STREAMING_MAX_UPLOAD_RETRIES} failed for file {file_index} ({file_name}): {e}")
                        if attempt < STREAMING_MAX_UPLOAD_RETRIES - 1:
                            # Exponential backoff: 5s, 10s, 20s
                            delay = 5 * (2 ** attempt)
                            LOGGER.info(f"Retrying in {delay} seconds...")
                            await sleep(delay)
                        else:
                            LOGGER.error(f"All upload attempts failed for file {file_index} ({file_name}). Skipping file.")
                
                # Set priority to 0 to prevent re-download
                try:
                    await TorrentManager.qbittorrent.torrents.file_prio(
                        hash=torrent_hash, id=[file_index], priority=0
                    )
                    LOGGER.info(f"Set priority to 0 for file {file_index} to prevent re-download.")
                except Exception as e:
                    LOGGER.error(f"Failed to set file priority: {e}")
                
                # Delete file if upload succeeded and file still exists
                if upload_success:
                    try:
                        # Remove .!qB extension if present
                        original_path = file_path.replace(".!qB", "")
                        
                        if await aiopath.exists(file_path):
                            await remove(file_path)
                            LOGGER.info(f"[STREAMING] Deleted uploaded file {file_index}: {file_name}")
                        elif await aiopath.exists(original_path):
                            await remove(original_path)
                            LOGGER.info(f"[STREAMING] Deleted uploaded file {file_index}: {file_name}")
                        else:
                            LOGGER.info(f"[STREAMING] File {file_index} already cleaned up by uploader")
                        stats['completed_files'].add(file_index)
                    except Exception as e:
                        LOGGER.error(f"[STREAMING] Failed to delete file {file_index} ({file_name}): {e}")
                else:
                    LOGGER.warning(f"[STREAMING] Skipped file {file_index} ({file_name}) due to upload failures.")
                
                # Remove from active downloads and ready set
                if file_index in stats['active_downloads']:
                    stats['active_downloads'].remove(file_index)
                if file_index in stats['ready_to_upload']:
                    stats['ready_to_upload'].remove(file_index)
                
                # Remove from upload tasks
                if file_index in stats['upload_tasks']:
                    del stats['upload_tasks'][file_index]
                
                # Remove from uploaders dict
                if file_index in stats['uploaders']:
                    del stats['uploaders'][file_index]
                
                # Bug #4 FIX: Update index to next pending file in priority order
                # CRITICAL: Don't use max(completed_files) + 1 as this skips files!
                # Example: If files 0,2,4 complete first, max+1=5 would skip files 1,3
                async with stats['index_lock']:
                    if stats.get('priority_order'):
                        # Find first file in priority order that's not completed
                        stats['index'] = len(stats['files'])  # Default to end
                        for idx in stats['priority_order']:
                            if idx not in stats['completed_files']:
                                stats['index'] = idx
                                break
                        
                        # Check if all files are done
                        if stats['index'] >= len(stats['files']):
                            LOGGER.info(f"All files processed for {self.name}")
                    else:
                        # Fallback if no priority order (shouldn't happen)
                        if stats['completed_files']:
                            stats['index'] = max(stats['completed_files']) + 1
                
                # Remove from progress tracking but first add to total
                if file_index in stats['current_file_progress']:
                    stats['total_uploaded_bytes'] += stats['current_file_progress'][file_index].get('total', 0)
                    del stats['current_file_progress'][file_index]
                
                # Trigger adaptive management to prioritize next files
                try:
                    await self._manage_adaptive_downloads(torrent_hash)
                except Exception as e:
                    LOGGER.error(f"[STREAMING] Failed to trigger adaptive management: {e}")

                # Trigger next upload
                await self._manage_uploads(torrent_hash)

                return upload_success
                
            except Exception as e:
                LOGGER.error(f"Critical error in upload handler for file {file_index}: {e}", exc_info=True)
                # Cleanup on error
                if file_index in stats['active_downloads']:
                    stats['active_downloads'].remove(file_index)
                if file_index in stats['upload_tasks']:
                    del stats['upload_tasks'][file_index]
                return False

    
    async def _manage_uploads(self, torrent_hash):
        """Manage uploads prioritizing sorted order."""
        stats = await self._get_streaming_stats(torrent_hash)
        
        # Clean finished tasks
        for file_idx, task in list(stats['upload_tasks'].items()):
            if task.done():
                del stats['upload_tasks'][file_idx]

        # Max 3 parallel uploads
        if len(stats['upload_tasks']) >= STREAMING_MAX_CONCURRENT_UPLOADS:
            return

        # Ensure priority order exists
        if not stats['priority_order'] and stats['files']:
             from ..ext_utils.bot_utils import natural_sort_key
             # Create default natural sort order of all file indices
             indexed_files = list(enumerate(stats['files']))
             indexed_files.sort(key=lambda x: natural_sort_key(x[1]))
             stats['priority_order'] = [x[0] for x in indexed_files]

        if len(stats['upload_tasks']) == 0:
             # Queue is empty, start the absolute first available priority file
             for file_idx in stats['priority_order']:
                 if (file_idx in stats['ready_to_upload'] and 
                     file_idx not in stats['completed_files']):
                     await self._start_upload_task(file_idx, torrent_hash)
                     return

        # Queue has items, check if we can start the next one (Overlap Logic)
        # Find the currently running task with the highest priority (lowest index)
        active_indices = [i for i in stats['upload_tasks'].keys()]
        # Sort by their position in the priority order
        # BUG FIX: Use float('inf') for missing items (sort last, not first)
        # Also use dict for O(1) lookup instead of O(n) list.index()
        priority_index_map = {idx: pos for pos, idx in enumerate(stats['priority_order'])}
        active_indices.sort(key=lambda x: priority_index_map.get(x, float('inf')))
        
        if not active_indices:
            return

        current_primary_idx = active_indices[0]
        current_task_info = stats['uploaders'].get(current_primary_idx)
        
        should_start_next = False
        if current_task_info:
            # Check progress of primary file
            # If > 80% uploaded, allow next file to start
            # TelegramUploader doesn't have total_size, so we skip this check and just allow parallel uploads
            if hasattr(current_task_info, 'processed_bytes') and hasattr(current_task_info, '_total_files'):
                # If we have progress info, check if substantial upload has happened
                if current_task_info.processed_bytes > 50 * 1024 * 1024:  # 50MB uploaded
                    should_start_next = True
                    LOGGER.info(f"[STREAMING] Primary upload {current_primary_idx} has uploaded {current_task_info.processed_bytes / (1024**2):.1f}MB, enabling parallel upload.")
            else:
                # For TelegramUploader without size tracking, just allow parallel after some time
                should_start_next = True
        
        if should_start_next and len(stats['upload_tasks']) < 3:
             # Find next available file in priority order
             start_idx = stats['priority_order'].index(current_primary_idx)
             for file_idx in stats['priority_order'][start_idx+1:]:
                 if (file_idx in stats['ready_to_upload'] and 
                     file_idx not in stats['upload_tasks'] and 
                     file_idx not in stats['completed_files']):
                     await self._start_upload_task(file_idx, torrent_hash)
                     break

    async def _start_upload_task(self, file_idx, torrent_hash):
         stats = await self._get_streaming_stats(torrent_hash)
         file_name = stats['files'][file_idx]
         LOGGER.info(f"[STREAMING] Starting prioritized upload for {file_name} (Index {file_idx})")
         path = f"{self.dir}/{file_name}"
         if not await aiopath.exists(path):
             path = f"{self.dir}/{file_name}.!qB"
         task = asyncio_create_task(self._upload_file_with_retry(file_idx, file_name, path, torrent_hash))
         stats['upload_tasks'][file_idx] = task

    async def _manage_adaptive_downloads(self, torrent_hash):
        """Manage adaptive bandwidth - prioritize additional files based on speeds."""
        from time import time as current_time
        from psutil import disk_usage
        
        stats = await self._get_streaming_stats(torrent_hash)
        now = current_time()
        
        # Get current speeds
        async with task_dict_lock:
            if self.mid in task_dict:
                dl_status = task_dict[self.mid]
                if isinstance(dl_status, StreamingStatus):
                    dl_speed_str = dl_status.download_status.speed() if hasattr(dl_status.download_status, 'speed') else "0B/s"
                    dl_speed = self._parse_speed(dl_speed_str)
                    
                    up_speed_str = dl_status.upload_status.speed() if dl_status.upload_status and hasattr(dl_status.upload_status, 'speed') else "0B/s"
                    up_speed = self._parse_speed(up_speed_str)
                    
                    # Store speed samples
                    stats['speeds']['download'].append((now, dl_speed))
                    stats['speeds']['upload'].append((now, up_speed))
                    
                    # Trim old samples (keep last 60 seconds)
                    stats['speeds']['download'] = [(t, s) for t, s in stats['speeds']['download'] if now - t < 60]
                    stats['speeds']['upload'] = [(t, s) for t, s in stats['speeds']['upload'] if now - t < 60]
        
        # Calculate average speeds
        avg_dl_speed = self._get_avg_speed(stats['speeds']['download'], now, window=30)
        
        # Ensure priority order exists
        if not stats['priority_order'] and stats['files']:
             from ..ext_utils.bot_utils import natural_sort_key
             indexed_files = list(enumerate(stats['files']))
             indexed_files.sort(key=lambda x: natural_sort_key(x[1]))
             stats['priority_order'] = [x[0] for x in indexed_files]
        
        # Iterate in priority order to find candidates
        active_and_uploading = set(stats['active_downloads']) | set(stats['upload_tasks'].keys()) | stats['completed_files'] | stats['ready_to_upload']
        
        # Use simple list comprehension on the PRIORITY ORDER list
        next_files = [idx for idx in stats['priority_order'] if idx not in active_and_uploading]
        
        if not next_files:
            return
        
        if next_files[0] < len(stats['files']):
            free_space = (await sync_to_async(disk_usage, DOWNLOAD_DIR)).free
            
            # Calculate target parallel downloads
            avg_file_size = self._estimate_avg_file_size(torrent_hash)
            max_by_disk = max(1, int(free_space / (avg_file_size * 1.5))) if avg_file_size > 0 else 1
            
            if free_space < 12 * 1024**3:
                # Bug Fix: Ensure at least 1 file can be active if we have > 4GB (checked later)
                # Previous logic min(len, 1) resulted in 0 if list was empty, stalling the pipeline
                target_parallel = 1
            else:
                is_saturated = self._is_download_saturated(avg_dl_speed, torrent_hash)
                if is_saturated:
                    target_parallel = len(stats['active_downloads'])
                else:
                    target_parallel = min(max_by_disk, 3)
            
            current_parallel = len(stats['active_downloads'])
            
            if current_parallel < target_parallel and free_space > 4 * 1024**3:
                files_to_add = target_parallel - current_parallel
                files_to_add = min(files_to_add, len(next_files))
                
                # Add new files
                for idx, file_idx in enumerate(next_files[:files_to_add]):
                    if file_idx < len(stats['files']) and file_idx not in stats['active_downloads']:
                        if stats.get('file_sizes') and file_idx < len(stats['file_sizes']):
                            file_size = stats['file_sizes'][file_idx]
                            free_space = (await sync_to_async(disk_usage, DOWNLOAD_DIR)).free
                            if free_space < file_size + 200 * 1024**2:
                                break
                        
                        stats['active_downloads'].append(file_idx)
                        LOGGER.info(f"[STREAMING] Adding file {file_idx} to active downloads.")

                # Apply Graded Priorities to ALL active downloads
                # Re-sort active downloads to ensure they match priority order
                active_sorted = sorted(stats['active_downloads'], 
                                     key=lambda x: stats['priority_order'].index(x) if x in stats['priority_order'] else 9999)
                
                if active_sorted:
                    # Highest Priority (First file) -> 1 (Normal Force)
                    high_prio = [active_sorted[0]]
                    await TorrentManager.qbittorrent.torrents.file_prio(hash=torrent_hash, id=high_prio, priority=1)
                    
                    # Others -> 1 (Normal)
                    if len(active_sorted) > 1:
                        normal_prio = active_sorted[1:]
                        await TorrentManager.qbittorrent.torrents.file_prio(hash=torrent_hash, id=normal_prio, priority=1)
                    
                    LOGGER.info(f"[STREAMING] Updated Priorities: High={high_prio} (P1), Normal={normal_prio if len(active_sorted) > 1 else 'None'}")
    
    async def _check_and_manage_disk_space(self, torrent_hash):
        """Bug #8: Check disk space and pause downloads if needed."""
        from psutil import disk_usage
        
        try:
            free_space = (await sync_to_async(disk_usage, DOWNLOAD_DIR)).free
            
            # If free space below 4GB, pause downloads and wait for uploads
            if free_space < 4 * 1024**3:
                LOGGER.warning(f"Low disk space: {free_space / 1024**3:.2f}GB. Pausing downloads...")
                
                # Pause torrent
                await TorrentManager.qbittorrent.torrents.pause([torrent_hash])
                
                # Wait for uploads to free space (check every 5 seconds)
                while free_space < 6 * 1024**3 and not self.is_cancelled:
                    await sleep(5)
                    free_space = (await sync_to_async(disk_usage, DOWNLOAD_DIR)).free
                    LOGGER.info(f"Waiting for space to free up... Current: {free_space / 1024**3:.2f}GB")
                
                # Resume torrent
                if not self.is_cancelled:
                    await TorrentManager.qbittorrent.torrents.resume([torrent_hash])
                    LOGGER.info("Disk space sufficient. Resuming downloads...")
        except Exception as e:
            LOGGER.error(f"Error in disk space management: {e}")
    

    
    async def _streaming_cleanup(self, torrent_hash):
        """Bug #6: Gracefully cleanup streaming task on cancellation."""
        LOGGER.info("Performing streaming cleanup...")
        stats = await self._get_streaming_stats(torrent_hash)
        
        # Stop all active downloads
        if stats['active_downloads']:
            # Set all active downloads to priority 0
            await TorrentManager.qbittorrent.torrents.file_prio(
                hash=torrent_hash, id=list(stats['active_downloads']), priority=0
            )
            LOGGER.info(f"Stopped {len(stats['active_downloads'])} active downloads")
        
        # Cancel all running upload tasks
        for file_idx, task in list(stats['upload_tasks'].items()):
            if not task.done():
                task.cancel()
        
        # Wait for cancellations
        if stats['upload_tasks']:
            await gather(*stats['upload_tasks'].values(), return_exceptions=True)
            LOGGER.info(f"Cancelled {len(stats['upload_tasks'])} upload tasks")
        
        # Clean up download directory
        from ..ext_utils.files_utils import clean_download
        await clean_download(self.dir)
        LOGGER.info("Cleaned up download directory")
        
        # Clear file path cache to free memory
        stats = await self._get_streaming_stats(torrent_hash)
        if stats.get('file_path_map'):
            cache_size = len(stats['file_path_map'])
            stats['file_path_map'].clear()
            LOGGER.info(f"Cleared {cache_size} cached file paths")
        
        # Clean up stats for this torrent
        await self._cleanup_streaming_stats(torrent_hash)

    def _parse_speed(self, speed_str):
        """Parse speed string like '1.5MB/s' to bytes per second."""
        try:
            # Remove /s and spaces
            speed_str = speed_str.strip().replace("/s", "").replace(" ", "")
            if not speed_str or speed_str == "0B":
                return 0
            
            multipliers = {
                'TB': 1024**4,
                'GB': 1024**3,
                'MB': 1024**2,
                'KB': 1024,
                'B': 1
            }
            
            # Check units in order (longest first to avoid matching 'B' in 'MB')
            for unit, multiplier in multipliers.items():
                if speed_str.endswith(unit):
                    number = float(speed_str[:-len(unit)])
                    return int(number * multiplier)
            
            return 0
        except Exception as e:
            LOGGER.error(f"[STREAMING] Failed to parse speed '{speed_str}': {e}")
            return 0
    
    def _get_avg_speed(self, speed_samples, current_time, window=30):
        """Calculate average speed from recent samples within time window."""
        if not speed_samples:
            return 0
        
        recent = [(t, s) for t, s in speed_samples if current_time - t <= window]
        if not recent:
            return 0
        
        return sum(s for _, s in recent) / len(recent)
    
    def _is_download_saturated(self, current_avg_speed, torrent_hash):
        """Detect if download bandwidth is saturated.
        
        Note: This is a sync method that needs to access streaming_stats.
        Uses defensive .get() calls to avoid race conditions.
        """
        # Defensive access with empty dict default
        stats = self.streaming_stats.get(torrent_hash, {})
        
        # If we have less than 2 active downloads, we're not saturated
        active_downloads = stats.get('active_downloads', [])
        if len(active_downloads) < 2:
            return False
        
        # Look at speed history - if speed hasn't increased despite adding files, we're saturated
        speeds = stats.get('speeds', {}).get('download', [])
        if len(speeds) < 10:
            return False  # Not enough data
        
        # Get speeds from when we had fewer active downloads
        try:
            older_speeds = [s for t, s in speeds if t < speeds[-1][0] - 15]
            if not older_speeds:
                return False
            
            avg_older = sum(older_speeds) / len(older_speeds)
            
            # If current speed is not significantly higher than before (< 10% increase), we're saturated
            if current_avg_speed < avg_older * 1.1:
                return True
        except (IndexError, KeyError, TypeError, ZeroDivisionError):
            # If we can't calculate, assume not saturated
            return False
        
        return False
    
    def _estimate_avg_file_size(self, torrent_hash):
        """Estimate average file size for parallel file calculation.
        
        Note: This is a sync method that needs to access streaming_stats.
        Uses defensive .get() calls to avoid race conditions.
        """
        try:
            # Defensive access with empty dict default
            stats = self.streaming_stats.get(torrent_hash, {})
            files = stats.get('files', [])
            
            # Calculate from actual data if available
            if self.size and len(files) > 0:
                return self.size / len(files)
            else:
                # Conservative default: 500MB per file
                return 500 * 1024**2
        except (AttributeError, TypeError, ZeroDivisionError):
            # Fallback to conservative estimate
            return 500 * 1024**2

    async def on_download_complete(self):
        await sleep(2)
        if self.is_cancelled:
            return

        if self.is_streaming:
            LOGGER.info(f"Streaming task finished: {self.name}. Waiting for uploads to complete...")
            
            # Get torrent hash once
            async with task_dict_lock:
                if self.mid in task_dict:
                    dl_status = task_dict[self.mid]
                    # Unwrap StreamingStatus if needed
                    if isinstance(dl_status, StreamingStatus):
                        torrent_hash = dl_status.download_status.hash()
                    else:
                        torrent_hash = dl_status.hash() if hasattr(dl_status, 'hash') else None
                else:
                    torrent_hash = None
            
            if not torrent_hash:
                LOGGER.error("Could not get torrent hash for streaming completion")
                return
            
            stats = await self._get_streaming_stats(torrent_hash)
            
            # Wait for all files to be processed (downloaded + uploaded or skipped)
            max_wait_time = 3600  # 1 hour safety timeout
            wait_start = time()
            
            while True:
                # Check if all SELECTED files have been processed
                if stats.get('priority_order'):
                    selected_files = set(stats['priority_order'])
                    total_selected = len(selected_files)
                    
                    # Count completed and uploading ONLY for selected files
                    completed = len([f for f in stats['completed_files'] if f in selected_files])
                    
                    uploading = 0
                    if stats['upload_tasks']:
                        uploading = len([f for f in stats['upload_tasks'] if f in selected_files])
                    
                    pending = total_selected - completed - uploading
                    
                    LOGGER.info(f"Streaming status: {completed}/{total_selected} uploaded (selected), {uploading} uploading, {pending} pending")
                    
                    if completed + uploading >= total_selected:
                        break
                else:
                    # Fallback for full torrent download
                    total_files = len(stats['files'])
                    completed = len(stats['completed_files'])
                    uploading = len(stats['upload_tasks'])
                    pending = total_files - completed - uploading
                    
                    LOGGER.info(f"Streaming status: {completed}/{total_files} uploaded, {uploading} uploading, {pending} pending")
                    
                    if completed + uploading >= total_files:
                        break
                
                if time() - wait_start > max_wait_time:
                    LOGGER.warning(f"Streaming timeout reached. Proceeding with upload termination.")
                    break
                
                if self.is_cancelled:
                    LOGGER.info("Streaming task cancelled during upload wait.")
                    return
                
                await sleep(5)
            
            # Wait for all outstanding active uploads to finish
            if stats['upload_tasks']:
                LOGGER.info(f"Waiting for {len(stats['upload_tasks'])} active uploads to complete...")
                await gather(*stats['upload_tasks'].values(), return_exceptions=True)
                LOGGER.info("All uploads completed.")
            
            # Build completion message
            if stats.get('priority_order'):
                # Selected files mode
                selected_files = set(stats['priority_order'])
                files_uploaded = len([f for f in stats['completed_files'] if f in selected_files])
                total_expected = len(selected_files)
            else:
                # Full torrent mode
                files_uploaded = len(stats['completed_files'])
                total_expected = len(stats['files'])
            
            files_failed = total_expected - files_uploaded
            
            msg = f"<b><i>{escape(self.name)}</i></b>\n│"
            msg += f"\n┟ <b>Files Uploaded</b> → {files_uploaded}/{total_expected}"
            if files_failed > 0:
                msg += f"\n┠ <b>Files Failed/Skipped</b> → {files_failed}"
            
            if stats.get('selected_size', 0) > 0:
                msg += f"\n┠ <b>Selected Downloaded</b> → {get_readable_file_size(stats['selected_size'])}"
            else:
                msg += f"\n┠ <b>Total Downloaded</b> → {get_readable_file_size(self.size)}"
                
            msg += f"\n┠ <b>Total Uploaded</b> → {get_readable_file_size(stats['total_uploaded_bytes'])}"
            msg += f"\n┠ <b>Time Taken</b> → {get_readable_time(time() - self.message.date.timestamp())}"
            msg += f"\n┖ <b>Task By</b> → {self.tag}\n\n"
            
            LOGGER.info(f"Streaming task complete: {files_uploaded}/{total_expected} files uploaded")
            
            # Send completion message
            await send_message(self.message, msg)
            
            # Cleanup stats for this torrent
            self._cleanup_streaming_stats(torrent_hash)
            
            # Cleanup
            async with task_dict_lock:
                if self.mid in task_dict:
                    del task_dict[self.mid]
            await self.clean()
            if self.pm_msg:
                await delete_message(self.pm_msg)
            await delete_status()
            return

        multi_links = False
        if (
            self.folder_name
            and self.same_dir
            and self.mid in self.same_dir[self.folder_name]["tasks"]
        ):
            async with same_directory_lock:
                while True:
                    async with task_dict_lock:
                        if self.mid not in self.same_dir[self.folder_name]["tasks"]:
                            return
                        if (
                            self.same_dir[self.folder_name]["total"] <= 1
                            or len(self.same_dir[self.folder_name]["tasks"]) > 1
                        ):
                            if self.same_dir[self.folder_name]["total"] > 1:
                                self.same_dir[self.folder_name]["tasks"].remove(
                                    self.mid
                                )
                                self.same_dir[self.folder_name]["total"] -= 1
                                spath = f"{self.dir}{self.folder_name}"
                                des_id = list(self.same_dir[self.folder_name]["tasks"])[
                                    0
                                ]
                                des_path = f"{DOWNLOAD_DIR}{des_id}{self.folder_name}"
                                LOGGER.info(f"Moving files from {self.mid} to {des_id}")
                                await move_and_merge(spath, des_path, self.mid)
                                multi_links = True
                            break
                    await sleep(1)
        async with task_dict_lock:
            if self.is_cancelled:
                return
            if self.mid not in task_dict:
                return
            download = task_dict[self.mid]
            self.name = download.name()
            gid = download.gid()
        LOGGER.info(f"Download completed: {self.name}")

        if not (self.is_torrent or self.is_qbit):
            self.seed = False

        if multi_links:
            self.seed = False
            await self.on_upload_error(
                f"{self.name} Downloaded!\n\nWaiting for other tasks to finish..."
            )
            return
        elif self.same_dir:
            self.seed = False

        if self.folder_name:
            self.name = self.folder_name.strip("/").split("/", 1)[0]

        if not await aiopath.exists(f"{self.dir}/{self.name}"):
            try:
                files = await listdir(self.dir)
                self.name = files[-1]
                if self.name == "yt-dlp-thumb":
                    self.name = files[0]
            except Exception as e:
                await self.on_upload_error(str(e))
                return

        dl_path = f"{self.dir}/{self.name}"
        # CRITICAL FIX: Don't recalculate size in streaming mode
        # In streaming, files are deleted as uploaded, so get_path_size would be wrong
        # We need to preserve the original torrent size set at initialization
        if not self.is_streaming:
            self.size = await get_path_size(dl_path)
        self.is_file = await aiopath.isfile(dl_path)

        if self.seed:
            up_dir = self.up_dir = f"{self.dir}10000"
            up_path = f"{self.up_dir}/{self.name}"
            await create_recursive_symlink(self.dir, self.up_dir)
            LOGGER.info(f"Shortcut created: {dl_path} -> {up_path}")
        else:
            up_dir = self.dir
            up_path = dl_path

        await remove_excluded_files(self.up_dir or self.dir, self.excluded_extensions)

        if not Config.QUEUE_ALL:
            async with queue_dict_lock:
                if self.mid in non_queued_dl:
                    non_queued_dl.remove(self.mid)
            await start_from_queued()

        if self.join and not self.is_file:
            await join_files(up_path)

        if self.extract and not self.is_nzb:
            up_path = await self.proceed_extract(up_path, gid)
            if self.is_cancelled:
                return
            self.is_file = await aiopath.isfile(up_path)
            self.name = up_path.replace(f"{up_dir}/", "").split("/", 1)[0]
            self.size = await get_path_size(up_dir)
            self.clear()
            await remove_excluded_files(up_dir, self.excluded_extensions)

        if self.ffmpeg_cmds:
            up_path = await self.proceed_ffmpeg(
                up_path,
                gid,
            )
            if self.is_cancelled:
                return
            self.is_file = await aiopath.isfile(up_path)
            self.name = up_path.replace(f"{up_dir}/", "").split("/", 1)[0]
            self.size = await get_path_size(up_dir)
            self.clear()

        if (
            (hasattr(self, "metadata_dict") and self.metadata_dict)
            or (hasattr(self, "audio_metadata_dict") and self.audio_metadata_dict)
            or (hasattr(self, "video_metadata_dict") and self.video_metadata_dict)
        ):
            up_path = await apply_metadata_title(
                self,
                up_path,
                gid,
                getattr(self, "metadata_dict", {}),
                getattr(self, "audio_metadata_dict", {}),
                getattr(self, "video_metadata_dict", {}),
            )
            if self.is_cancelled:
                return

            self.name = up_path.replace(f"{up_dir.rstrip('/')}/", "").split("/", 1)[0]
            self.size = await get_path_size(up_path)
            self.clear()

        if self.is_leech and self.is_file:
            fname = ospath.basename(up_path)
            self.file_details["filename"] = fname
            self.file_details["mime_type"] = (guess_type(fname))[
                0
            ] or "application/octet-stream"

        if self.name_swap:
            up_path = await self.substitute(up_path)
            if self.is_cancelled:
                return
            self.is_file = await aiopath.isfile(up_path)
            self.name = up_path.replace(f"{up_dir}/", "").split("/", 1)[0]

        if self.screen_shots:
            up_path = await self.generate_screenshots(up_path)
            if self.is_cancelled:
                return
            self.is_file = await aiopath.isfile(up_path)
            self.name = up_path.replace(f"{up_dir}/", "").split("/", 1)[0]
            self.size = await get_path_size(up_dir)

        if self.convert_audio or self.convert_video:
            up_path = await self.convert_media(
                up_path,
                gid,
            )
            if self.is_cancelled:
                return
            self.is_file = await aiopath.isfile(up_path)
            self.name = up_path.replace(f"{up_dir}/", "").split("/", 1)[0]
            self.size = await get_path_size(up_dir)
            self.clear()

        if self.sample_video:
            up_path = await self.generate_sample_video(up_path, gid)
            if self.is_cancelled:
                return
            self.is_file = await aiopath.isfile(up_path)
            self.name = up_path.replace(f"{up_dir}/", "").split("/", 1)[0]
            self.size = await get_path_size(up_dir)
            self.clear()

        if self.compress:
            up_path = await self.proceed_compress(
                up_path,
                gid,
            )
            self.is_file = await aiopath.isfile(up_path)
            if self.is_cancelled:
                return
            self.clear()

        self.name = up_path.replace(f"{up_dir}/", "").split("/", 1)[0]
        self.size = await get_path_size(up_dir)

        if self.is_leech and not self.compress:
            await self.proceed_split(up_path, gid)
            if self.is_cancelled:
                return
            self.clear()

        self.subproc = None

        add_to_queue, event = await check_running_tasks(self, "up")
        await start_from_queued()
        if add_to_queue:
            LOGGER.info(f"Added to Queue/Upload: {self.name}")
            async with task_dict_lock:
                task_dict[self.mid] = QueueStatus(self, gid, "Up")
            await event.wait()
            if self.is_cancelled:
                return
            LOGGER.info(f"Start from Queued/Upload: {self.name}")

        self.size = await get_path_size(up_dir)

        if self.is_yt:
            LOGGER.info(f"Up to yt Name: {self.name}")
            yt = YouTubeUpload(self, up_path)
            async with task_dict_lock:
                task_dict[self.mid] = YtStatus(self, yt, gid, "up")
            await gather(
                update_status_message(self.message.chat.id),
                sync_to_async(yt.upload),
            )
            del yt
        elif self.is_leech:
            LOGGER.info(f"Leech Name: {self.name}")
            tg = TelegramUploader(self, up_dir)
            async with task_dict_lock:
                task_dict[self.mid] = TelegramStatus(self, tg, gid, "up")
            await gather(
                update_status_message(self.message.chat.id),
                tg.upload(),
            )
            del tg

        elif is_gdrive_id(self.up_dest):
            LOGGER.info(f"Gdrive Upload Name: {self.name}")
            drive = GoogleDriveUpload(self, up_path)
            async with task_dict_lock:
                task_dict[self.mid] = GoogleDriveStatus(self, drive, gid, "up")
            await gather(
                update_status_message(self.message.chat.id),
                sync_to_async(drive.upload),
            )
            del drive
        else:
            LOGGER.info(f"Rclone Upload Name: {self.name}")
            RCTransfer = RcloneTransferHelper(self)
            async with task_dict_lock:
                task_dict[self.mid] = RcloneStatus(self, RCTransfer, gid, "up")
            await gather(
                update_status_message(self.message.chat.id),
                RCTransfer.upload(up_path),
            )
            del RCTransfer
        return

    async def on_upload_complete(
        self, link, files, folders, mime_type, rclone_path="", dir_id=""
    ):
        if (
            self.is_super_chat
            and Config.INCOMPLETE_TASK_NOTIFIER
            and Config.DATABASE_URL
        ):
            await database.rm_complete_task(self.message.link)
        msg = (
            f"<b><i>{escape(self.name)}</i></b>\n│"
            f"\n┟ <b>Task Size</b> → {get_readable_file_size(self.size)}"
            f"\n┠ <b>Time Taken</b> → {get_readable_time(time() - self.message.date.timestamp())}"
            f"\n┠ <b>In Mode</b> → {self.mode[0]}"
            f"\n┠ <b>Out Mode</b> → {self.mode[1]}"
        )
        LOGGER.info(f"Task Done: {self.name}")
        if self.is_yt:
            buttons = ButtonMaker()
            if mime_type == "Folder/Playlist":
                msg += "\n┠ <b>Type</b> → Playlist"
                msg += f"\n┖ <b>Total Videos</b> → {files}"
                if link:
                    buttons.url_button("🔗 View Playlist", link)
                user_message = f"{self.tag}\nYour playlist ({files} videos) has been uploaded to YouTube successfully!"
            else:
                msg += "\n┖ <b>Type</b> → Video"
                if link:
                    buttons.url_button("🔗 View Video", link)
                user_message = (
                    f"{self.tag}\nYour video has been uploaded to YouTube successfully!"
                )

            msg += f"\n\n<b>Task By: </b>{self.tag}"

            button = buttons.build_menu(1) if link else None

            await send_message(self.user_id, msg, button)
            if Config.LEECH_DUMP_CHAT:
                await send_message(int(Config.LEECH_DUMP_CHAT), msg, button)
            await send_message(self.message, user_message, button)

        elif self.is_leech:
            msg += f"\n<b>Total Files: </b>{folders}"
            if mime_type != 0:
                msg += f"\n┠ <b>Corrupted Files</b> → {mime_type}"
            msg += f"\n┖ <b>Task By</b> → {self.tag}\n\n"

            if self.bot_pm:
                pmsg = msg
                pmsg += "〶 <b><u>Action Performed :</u></b>\n"
                pmsg += "⋗ <i>File(s) have been sent to User PM</i>\n\n"
                if self.is_super_chat:
                    await send_message(self.message, pmsg)

            if not files and not self.is_super_chat:
                await send_message(self.message, msg)
            else:
                log_chat = self.user_id if self.bot_pm else self.message
                msg += "〶 <b><u>Files List :</u></b>\n"
                fmsg = ""
                for index, (link, name) in enumerate(files.items(), start=1):
                    chat_id, msg_id = link.split("/")[-2:]
                    fmsg += f"{index}. <a href='{link}'>{name}</a>"
                    if Config.MEDIA_STORE and (
                        self.is_super_chat or Config.LEECH_DUMP_CHAT
                    ):
                        if chat_id.isdigit():
                            chat_id = f"-100{chat_id}"
                        flink = f"https://t.me/{TgClient.BNAME}?start={encode_slink('file' + chat_id + '&&' + msg_id)}"
                        fmsg += f"\n┖ <b>Get Media</b> → <a href='{flink}'>Store Link</a> | <a href='https://t.me/share/url?url={flink}'>Share Link</a>"
                    fmsg += "\n"
                    if len(fmsg.encode() + msg.encode()) > 4000:
                        await send_message(log_chat, msg + fmsg)
                        await sleep(1)
                        fmsg = ""
                if fmsg != "":
                    await send_message(log_chat, msg + fmsg)
        else:
            msg += f"\n│\n┟ <b>Type</b> → {mime_type}"
            if mime_type == "Folder":
                msg += f"\n┠ <b>SubFolders</b> → {folders}"
                msg += f"\n┠ <b>Files</b> → {files}"

            multi_link_msg = ""
            multi_links = []
            if isinstance(link, dict) and not self.is_yt:

                for service, result in link.items():
                    if "error" in result:
                        multi_link_msg += (
                            f"{service.capitalize()}: Error - {result['error']}\n"
                        )
                    elif result.get("link"):
                        multi_links.append(
                            (f"{service.capitalize()} Link", result["link"])
                        )
                multi_link_msg = multi_link_msg.strip()
                link = None  # Disable single link button logic

            if (
                link
                or rclone_path
                and Config.RCLONE_SERVE_URL
                and not self.private_link
                or multi_links
            ):
                buttons = ButtonMaker()
                if link and Config.SHOW_CLOUD_LINK:
                    buttons.url_button("☁️ Cloud Link", link)
                elif multi_links:
                    for name, url in multi_links:
                        buttons.url_button(name, url)
                else:
                    msg += f"\n\nPath: <code>{rclone_path}</code>"
                if rclone_path and Config.RCLONE_SERVE_URL and not self.private_link:
                    remote, rpath = rclone_path.split(":", 1)
                    url_path = rutils.quote(f"{rpath}")
                    share_url = f"{Config.RCLONE_SERVE_URL}/{remote}/{url_path}"
                    if mime_type == "Folder":
                        share_url += "/"
                    buttons.url_button("🔗 Rclone Link", share_url)
                if not rclone_path and dir_id:
                    INDEX_URL = ""
                    if self.private_link:
                        INDEX_URL = self.user_dict.get("INDEX_URL", "") or ""
                    elif Config.INDEX_URL:
                        INDEX_URL = Config.INDEX_URL
                    if INDEX_URL and self.name:
                        safe_name = rutils.quote(self.name.strip("/"))
                        share_url = f"{INDEX_URL}/{safe_name}"
                        buttons.url_button("⚡ Index Link", share_url)
                        if mime_type.startswith(("image", "video", "audio")):
                            share_urls = f"{share_url}?a=view"
                            buttons.url_button("🌐 View Link", share_urls)
                button = buttons.build_menu(2)
            else:
                if not multi_link_msg:
                    msg += f"\n┃\n┠ Path: <code>{rclone_path}</code>"
                button = None
            msg += f"\n┃\n┖ <b>Task By</b> → {self.tag}\n\n"
            group_msg = (
                msg + "〶 <b><u>Action Performed :</u></b>\n"
                "⋗ <i>Cloud link(s) have been sent to User PM</i>\n\n"
            )

            if multi_link_msg:
                group_msg += multi_link_msg + "\n"
                msg += multi_link_msg + "\n"

            if self.bot_pm and self.is_super_chat:
                await send_message(self.user_id, msg, button)

            if hasattr(Config, "MIRROR_LOG_ID") and Config.MIRROR_LOG_ID:
                await send_message(Config.MIRROR_LOG_ID, msg, button)

            await send_message(self.message, group_msg, button)
        if self.seed:
            await clean_target(self.up_dir)
            async with queue_dict_lock:
                if self.mid in non_queued_up:
                    non_queued_up.remove(self.mid)
            await start_from_queued()
            return

        if self.pm_msg and (not Config.DELETE_LINKS or Config.CLEAN_LOG_MSG):
            await delete_message(self.pm_msg)

        await clean_download(self.dir)
        async with task_dict_lock:
            if self.mid in task_dict:
                del task_dict[self.mid]
            count = len(task_dict)
        if count == 0:
            await self.clean()
        else:
            await update_status_message(self.message.chat.id)

        async with queue_dict_lock:
            if self.mid in non_queued_up:
                non_queued_up.remove(self.mid)

        await start_from_queued()

    async def on_download_error(self, error, button=None, is_limit=False):
        async with task_dict_lock:
            if self.mid in task_dict:
                del task_dict[self.mid]
            count = len(task_dict)
        await self.remove_from_same_dir()
        msg = (
            f"""〶 <b><i><u>Limit Breached:</u></i></b>
│
┟ <b>Task Size</b> → {get_readable_file_size(self.size)}
┠ <b>In Mode</b> → {self.mode[0]}
┠ <b>Out Mode</b> → {self.mode[1]}
{error}"""
            if is_limit
            else f"""<i><b>〶 Download Stopped!</b></i>
│
┟ <b>Due To</b> → {escape(str(error))}
┠ <b>Task Size</b> → {get_readable_file_size(self.size)}
┠ <b>Time Taken</b> → {get_readable_time(time() - self.message.date.timestamp())}
┠ <b>In Mode</b> → {self.mode[0]}
┠ <b>Out Mode</b> → {self.mode[1]}
┖ <b>Task By</b> → {self.tag}"""
        )

        await send_message(self.message, msg, button)
        if count == 0:
            await self.clean()
        else:
            await update_status_message(self.message.chat.id)

        if (
            self.is_super_chat
            and Config.INCOMPLETE_TASK_NOTIFIER
            and Config.DATABASE_URL
        ):
            await database.rm_complete_task(self.message.link)

        async with queue_dict_lock:
            if self.mid in queued_dl:
                queued_dl[self.mid].set()
                del queued_dl[self.mid]
            if self.mid in queued_up:
                queued_up[self.mid].set()
                del queued_up[self.mid]
            if self.mid in non_queued_dl:
                non_queued_dl.remove(self.mid)
            if self.mid in non_queued_up:
                non_queued_up.remove(self.mid)

        await start_from_queued()
        await sleep(3)
        await clean_download(self.dir)
        if self.up_dir:
            await clean_download(self.up_dir)
        if self.thumb and await aiopath.exists(self.thumb):
            await remove(self.thumb)

    async def on_upload_error(self, error):
        async with task_dict_lock:
            if self.mid in task_dict:
                del task_dict[self.mid]
            count = len(task_dict)
        await send_message(self.message, f"{self.tag} {escape(str(error))}")
        if count == 0:
            await self.clean()
        else:
            await update_status_message(self.message.chat.id)

        if (
            self.is_super_chat
            and Config.INCOMPLETE_TASK_NOTIFIER
            and Config.DATABASE_URL
        ):
            await database.rm_complete_task(self.message.link)

        async with queue_dict_lock:
            if self.mid in queued_dl:
                queued_dl[self.mid].set()
                del queued_dl[self.mid]
            if self.mid in queued_up:
                queued_up[self.mid].set()
                del queued_up[self.mid]
            if self.mid in non_queued_dl:
                non_queued_dl.remove(self.mid)
            if self.mid in non_queued_up:
                non_queued_up.remove(self.mid)

        await start_from_queued()
        await sleep(3)
        await clean_download(self.dir)
        if self.up_dir:
            await clean_download(self.up_dir)
        if self.thumb and await aiopath.exists(self.thumb):
            await remove(self.thumb)
