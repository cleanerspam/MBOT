from time import time
from os import path as ospath
from .... import LOGGER
from ...ext_utils.status_utils import (
    MirrorStatus,
    get_readable_file_size,
    get_readable_time,
    get_progress_bar_string,
    get_raw_time,
    speed_string_to_bytes,
)

class StreamingStatus:
    def __init__(self, listener, download_status):
        self.listener = listener
        self.download_status = download_status
        self.upload_status = None
        self.engine = download_status.engine
        self._torrent_hash = download_status.hash()  # Cache hash immediately
        
        
    @property
    def streaming_stats(self):
        return self.listener.streaming_stats.get(self._torrent_hash, {})
        
        # Add (Streaming) tag to mode if not present
        if self.listener.mode and "(Streaming)" not in self.listener.mode[0]:
            new_in_mode = self.listener.mode[0] + " (Streaming)"
            self.listener.mode = (new_in_mode, self.listener.mode[1])
    
    def _calculate_selected_size(self):
        """Calculate selected size from priority order and file sizes.
        
        Returns the calculated size without modifying streaming_stats.
        This is a read-only helper to avoid side effects in display methods.
        """
        # Return cached value if available
        if self.streaming_stats.get('selected_size', 0) > 0:
            return self.streaming_stats['selected_size']
        
        # Calculate on-the-fly for display purposes only
        priority_order = self.streaming_stats.get('priority_order', [])
        file_sizes = self.streaming_stats.get('file_sizes', [])
        
        if not priority_order or not file_sizes:
            return 0
        
        # Bug #6 Fix: Add explicit bounds checking to prevent IndexError
        return sum(file_sizes[i] for i in priority_order if 0 <= i < len(file_sizes))

    def progress(self):
        """Return overall progress as percentage string based on data size."""
        try:
            # Bug #17 Fix: Initialize total_uploaded before use
            total_uploaded = 0
            
            # Bug #16 Fix: Use self.streaming_stats instead of undefined 'stats'
            # Bug #5 Fix: Don't double-count - completed files are already in total_uploaded_bytes
            # Only count bytes from files that have completed upload (removed from progress tracking)
            completed_files = self.streaming_stats.get('completed_files', set())
            if not isinstance(completed_files, set):
                completed_files = set(completed_files) if completed_files else set()
            
            # Count bytes from completed uploads (these are removed from current_file_progress)
            if completed_files:
                file_sizes = self.streaming_stats.get('file_sizes', [])
                priority_order = self.streaming_stats.get('priority_order', [])
                for idx in completed_files:
                    if idx in priority_order and 0 <= idx < len(file_sizes):
                        total_uploaded += file_sizes[idx]
            if self.streaming_stats.get('uploaders'):
                # Bug #32 Fix: Safe attribute access for uploader properties
                total_uploaded += sum(getattr(u, 'processed_bytes', 0) or 0 for u in self.streaming_stats['uploaders'].values())
            
            # Use selected size if available (file selection mode), otherwise full torrent size
            total = self.streaming_stats.get('selected_size', self.listener.size)
            if total > 0:
                p = min((total_uploaded / total) * 100, 100.0)  # Cap at 100%
                return f"{round(p, 2)}%"
            return "0%"
        except (AttributeError, KeyError, TypeError, ZeroDivisionError) as e:
            LOGGER.error(f"Error calculating streaming progress: {e}")
            return "0%"

    def task(self):
        return self

    def processed_bytes(self):
        # Bug #5 Fix: Calculate upload bytes correctly from completed files
        total_uploaded = 0
        completed_files = self.streaming_stats.get('completed_files', set())
        if not isinstance(completed_files, set):
            completed_files = set(completed_files) if completed_files else set()
        
        # Add bytes from fully completed uploads
        if completed_files:
            file_sizes = self.streaming_stats.get('file_sizes', [])
            priority_order = self.streaming_stats.get('priority_order', [])
            for idx in completed_files:
                if idx in priority_order and 0 <= idx < len(file_sizes):
                    total_uploaded += file_sizes[idx]
        
        # Add in-progress upload bytes
        if self.streaming_stats.get('uploaders'):
            # Bug #32 Fix: Safe attribute access for uploader properties
            total_uploaded += sum(getattr(u, 'processed_bytes', 0) or 0 for u in self.streaming_stats['uploaders'].values())
            
        up_processed = get_readable_file_size(total_uploaded)
        dl_processed = self.download_status.processed_bytes()
        return f"D: {dl_processed} | U: {up_processed}"

    def speed(self):
        return self.download_status.speed()

    def name(self):
        return self.listener.name

    def size(self):
        # Show selected size if available (file selection), otherwise full torrent size
        selected_size = self.streaming_stats.get('selected_size', 0)
        
        if selected_size > 0:
            selected = get_readable_file_size(selected_size)
            total = get_readable_file_size(self.listener.size)
            return f"{selected} (of {total})"
        
        return get_readable_file_size(self.listener.size)

    def eta(self):
        try:
            # Bug #30 Fix: Use selected size if available for accurate ETA in streaming mode
            total_size = self.streaming_stats.get('selected_size', self.listener.size)
            if total_size <= 0:
                return "-"
            
            # Bug #5 Fix: Calculate total uploaded correctly
            total_uploaded = 0
            completed_files = self.streaming_stats.get('completed_files', set())
            if not isinstance(completed_files, set):
                completed_files = set(completed_files) if completed_files else set()
            
            if completed_files:
                file_sizes = self.streaming_stats.get('file_sizes', [])
                priority_order = self.streaming_stats.get('priority_order', [])
                for idx in completed_files:
                    if idx in priority_order and 0 <= idx < len(file_sizes):
                        total_uploaded += file_sizes[idx]
            if self.streaming_stats.get('uploaders'):
                # Bug #32 Fix: Safe attribute access for uploader properties
                total_uploaded += sum(getattr(u, 'processed_bytes', 0) or 0 for u in self.streaming_stats['uploaders'].values())
            
            remaining = total_size - total_uploaded
            if remaining <= 0:
                return "0s"
            
            # Get speeds
            dl_speed = speed_string_to_bytes(self.download_status.speed())
            
            up_speed = 0
            if self.streaming_stats.get('uploaders'):
                # Bug #32 Fix: Safe attribute access for uploader properties
                up_speed = sum(getattr(u, 'speed', 0) or 0 for u in self.streaming_stats['uploaders'].values())
            
            if dl_speed == 0 and up_speed == 0:
                return "-"
            
            if dl_speed == 0:
                # Downloading paused or finished
                speed = up_speed
            elif up_speed == 0:
                # Uploading hasn't started or stalled
                speed = dl_speed 
            else:
                # Both active, bottleneck rules apply
                speed = min(dl_speed, up_speed)
                
            if speed > 0:
                seconds = remaining / speed
                return get_readable_time(seconds)
            return "-"

        except Exception as e:
            LOGGER.error(f"ETA Error: {e}")
            return "-"

    async def status(self):
        # Ensure inner status is updated
        await self.download_status.update()
        return MirrorStatus.STATUS_DOWNLOAD

    def seeders_num(self):
        return self.download_status.seeders_num()

    def leechers_num(self):
        return self.download_status.leechers_num()

    async def update(self):
        """Update both download and upload status."""
        await self.download_status.update()
        if self.upload_status and hasattr(self.upload_status, 'update'):
            await self.upload_status.update()

    def gid(self):
        try:
            return self.download_status.gid()
        except (AttributeError, Exception) as e:
            LOGGER.warning(f"Unable to get gid, using fallback: {e}")
            return f"{self.listener.mid}"[:12]

    def hash(self):
        return self.download_status.hash()
    
    async def cancel_task(self):
        """Delegate cancellation to the inner download status"""
        if hasattr(self.download_status, 'cancel_task'):
            await self.download_status.cancel_task()
        else:
            self.listener.is_cancelled = True

    def _truncate(self, name, limit=40):
        if len(name) <= limit:
            return name
        return f"{name[:limit//2-2]}...{name[-(limit//2-1):]}"

    def get_custom_status(self):
        """Clean minimal status layout for Streaming Mode."""
        name = self._truncate(self.listener.name, 50)
        msg = f"\n<b>{name}</b>"
        
        # Sizes
        # Logic for Size
        # If selected_size is missing or 0, calculate it dynamically from priority_order
        # Calculate selected size if not already set
        # NOTE: This is now done during initialization or file selection, not here
        # Keeping this as a fallback for backwards compatibility
        selected_size_value = self._calculate_selected_size()
        
        if selected_size_value > 0:
            total_size = get_readable_file_size(self.listener.size)
            selected_size = get_readable_file_size(selected_size_value)
            msg += f"\n┠ Total Size: {total_size}"
            msg += f"\n┠ Selected Size: {selected_size}"
        else:
            total_size = get_readable_file_size(self.listener.size)
            msg += f"\n┠ Total Size: {total_size}"
        
        # File counts
        priority_order = self.streaming_stats.get('priority_order', [])
        total_selected = len(priority_order)
        
        # Bug #11 Fix: Ensure completed_files is always a set with type validation
        completed_files = self.streaming_stats.get('completed_files', set())
        if not isinstance(completed_files, set):
            completed_files = set(completed_files) if completed_files else set()
        
        # Bug #15 Fix: Convert priority_order to set for O(1) lookups instead of O(n)
        priority_order_set = set(priority_order) if priority_order else set()
        
        # Downloaded = files that are fully downloaded AND in selected files
        # Only count files that are in the priority_order (selected files)
        downloaded_indices = {i for i in completed_files if i in priority_order_set}
    
        # Add files currently being uploaded (download completed, upload in progress)
        upload_tasks = self.streaming_stats.get('upload_tasks', {})
        if upload_tasks:
            for idx in upload_tasks.keys():
                if idx in priority_order_set:  # Only count if selected
                    downloaded_indices.add(idx)
        
        # Also check current_file_progress for completed downloads that might not be in the other sets yet
        current_file_progress = self.streaming_stats.get('current_file_progress', {})
        if current_file_progress:
            for idx, data in current_file_progress.items():
                if idx in priority_order_set and data.get('downloaded', 0) >= data.get('total', 1) * 0.999:
                    downloaded_indices.add(idx)

        downloaded_count = len(downloaded_indices)
        # Bug #33 Fix: Handle edge case when no files selected
        if total_selected > 0:
            # Ensure count never exceeds total (safety cap)
            downloaded_count = min(downloaded_count, total_selected)
        else:
            downloaded_count = 0
        
        msg += f"\n┠ Downloaded: <b>{downloaded_count}/{total_selected}</b> files"
        
        # Uploaded = files completed AND in selected files
        uploaded_indices = {i for i in completed_files if i in priority_order_set}
        uploaded_count = len(uploaded_indices)
        # Bug #33 Fix: Handle edge case when no files selected
        if total_selected > 0:
            # Ensure count never exceeds total
            uploaded_count = min(uploaded_count, total_selected)
        else:
            uploaded_count = 0
        msg += f"\n┠ Uploaded: <b>{uploaded_count}/{total_selected}</b> files"
        
        # Seeders and Leechers
        seeders = self.seeders_num()
        leechers = self.leechers_num()
        msg += f"\n┠ Seeders: <b>{seeders}</b> | Leechers: <b>{leechers}</b>"
        
        # Downloading section
        active_downloads = self.streaming_stats.get('active_downloads', [])
        if active_downloads:
            dl_speed = self.download_status.speed()
            msg += f"\n┠ <b>DOWNLOADING</b> ({dl_speed})"
            
            files = self.streaming_stats.get('files', [])
            current_file_progress = self.streaming_stats.get('current_file_progress', {})
            for file_idx in active_downloads:
                if file_idx < len(files):
                    fname = files[file_idx]
                    basename = self._get_basename(fname)
                    
                    prog_info = current_file_progress.get(file_idx, {})
                    # Bug #7 Fix: Consistent .get() pattern to prevent division by zero
                    total_size = prog_info.get('total', 0)
                    if total_size > 0:
                        percent = int((prog_info.get('downloaded', 0) / total_size) * 100)
                        size_str = get_readable_file_size(total_size)
                        msg += f"\n┃  <small>{basename}</small> {percent}% / {size_str}"
                    else:
                        msg += f"\n┃  <small>{basename}</small> 0%"
                else:
                    # Bug #14 Fix: Add proper else handling for out-of-bounds indices
                    LOGGER.warning(f"File index {file_idx} out of bounds (files={len(files)})")
                    msg += f"\n┃  <small>File {file_idx}</small> waiting..."
        
        # Uploading section
        upload_tasks = self.streaming_stats.get('upload_tasks', {})
        if upload_tasks:
            up_speed = 0
            uploaders = self.streaming_stats.get('uploaders', {})
            if uploaders:
                # Bug #32 Fix: Safe attribute access for uploader properties
                up_speed = sum(getattr(u, 'speed', 0) or 0 for u in uploaders.values())
            
            speed_str = get_readable_file_size(up_speed) + "/s"
            msg += f"\n┠ <b>UPLOADING</b> ({speed_str})"
            
            files = self.streaming_stats.get('files', [])
            # Bug #23 Fix: Add consistent type validation for completed_files
            completed_files = self.streaming_stats.get('completed_files', set())
            if not isinstance(completed_files, set):
                completed_files = set(completed_files) if completed_files else set()
            current_file_progress = self.streaming_stats.get('current_file_progress', {})
            
            for file_idx, task in upload_tasks.items():
                if file_idx in completed_files:
                    continue
                    
                if file_idx < len(files):
                    fname = files[file_idx]
                    basename = self._get_basename(fname)
                    
                    if uploaders and file_idx in uploaders:
                        uploader = uploaders[file_idx]
                        file_size = current_file_progress.get(file_idx, {}).get('total', 0)
                        if file_size > 0:
                            try:
                                # Bug #25 Fix: Add try/except for potential AttributeError
                                percent = int((uploader.processed_bytes / file_size) * 100)
                                size_str = get_readable_file_size(file_size)
                                msg += f"\n┃  <small>{basename}</small> {percent}% / {size_str}"
                            except (AttributeError, TypeError) as e:
                                LOGGER.warning(f"Error calculating upload progress for file {file_idx}: {e}")
                                msg += f"\n┃  <small>{basename}</small> preparing..."
                        else:
                            msg += f"\n┃  <small>{basename}</small> preparing..."
                    else:
                        msg += f"\n┃  <small>{basename}</small> preparing..."
        
        # Overall progress bars - based on actual data size for accuracy
        total_selected = len(self.streaming_stats.get('priority_order', []))
        
        if total_selected > 0:
            # Download progress = calculate based on bytes downloaded vs total selected size
            total_downloaded_bytes = 0
            
            # Add bytes from completed files (fully uploaded)
            # These files are removed from current_file_progress, so we must add their size
            total_downloaded_bytes = self.streaming_stats.get('total_uploaded_bytes', 0)
            
            # Bug #36 Fix: Use file_sizes for correct size lookup of fully downloaded files
            # Files in 'upload_tasks' and 'ready_to_upload' are fully downloaded but removed from 'current_file_progress'
            file_sizes = self.streaming_stats.get('file_sizes', [])
            
            # Add bytes from files currently uploading (they are fully downloaded)
            upload_tasks = self.streaming_stats.get('upload_tasks', {})
            if upload_tasks:
                for file_idx in upload_tasks.keys():
                    # Check bounds safely
                    if 0 <= file_idx < len(file_sizes):
                        total_downloaded_bytes += file_sizes[file_idx]
            
            # Add bytes from files ready to upload (fully downloaded but not started uploading yet)
            ready_list = self.streaming_stats.get('ready_to_upload', [])
            if ready_list:
                for file_idx in ready_list:
                    # Check bounds safely
                    if 0 <= file_idx < len(file_sizes):
                        total_downloaded_bytes += file_sizes[file_idx]

            # Add partial bytes from currently downloading files
            # Bug #22 Fix: Use .get() to prevent KeyError
            for file_idx in self.streaming_stats.get('active_downloads', []):
                if file_idx in self.streaming_stats.get('current_file_progress', {}):
                    prog_info = self.streaming_stats['current_file_progress'][file_idx]
                    total_downloaded_bytes += prog_info.get('downloaded', 0)
            
            # Use selected size if available
            total_size = self.streaming_stats.get('selected_size', self.listener.size)
            
            # Optimization: If all files are downloaded, force 100%
            # This fixes the issue where byte summation misses padding or rounding errors
            if downloaded_count == total_selected and total_selected > 0:
                dl_percent = 100.0
            elif total_size > 0:
                dl_percent = (total_downloaded_bytes / total_size) * 100
                dl_percent = min(dl_percent, 100.0)
            else:
                dl_percent = 0
        else:
            dl_percent = 0
        
        # msg += f"\n┠ ▼ {dl_percent:5.1f}% Download"
        
        # Upload progress = calculate based on bytes uploaded vs total selected size
        if total_selected > 0:
            total_uploaded_bytes = 0
            
            # Add bytes from completed files (accumulated in stats)
            total_uploaded_bytes = self.streaming_stats.get('total_uploaded_bytes', 0)
            
            # Add partial bytes from currently uploading files
            if self.streaming_stats.get('uploaders'):
                # Bug #26 Fix: Use .get() to prevent KeyError
                completed_files_set = self.streaming_stats.get('completed_files', set())
                if not isinstance(completed_files_set, set):
                    completed_files_set = set(completed_files_set) if completed_files_set else set()
                
                for file_idx, uploader in self.streaming_stats['uploaders'].items():
                    # Only add if not already counted in total_uploaded_bytes
                    # (Though uploaders should be removed when completed)
                    if file_idx not in completed_files_set:
                        try:
                            total_uploaded_bytes += uploader.processed_bytes
                        except (AttributeError, TypeError) as e:
                            LOGGER.warning(f"Error accessing uploader bytes for file {file_idx}: {e}")
            
            # Use selected size if available
            total_size = self.streaming_stats.get('selected_size', self.listener.size)
            
            # Optimization: If all files are uploaded, force 100%
            if uploaded_count == total_selected and total_selected > 0:
                up_percent = 100.0
            elif total_size > 0:
                up_percent = (total_uploaded_bytes / total_size) * 100
                up_percent = min(up_percent, 100.0)
            else:
                up_percent = 0.0
        else:
            up_percent = 0.0
        
        up_bar = get_progress_bar_string(f"{up_percent:.1f}%")
        msg += f"\n┠ ▲ {up_percent:5.1f}% {up_bar} Upload"
        
        # ETA
        msg += f"\n┖ ETA: {self.eta()}"
        
        return msg

    def _get_basename(self, path):
        return ospath.basename(path)
