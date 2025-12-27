from logging import getLogger

from .helper import GoogleDriveHelper

LOGGER = getLogger(__name__)


class GoogleDriveCount(GoogleDriveHelper):
    """Minimal implementation to count Google Drive files for download."""
    
    def __init__(self):
        super().__init__()
    
    def count(self, link, user_id):
        """Count and get metadata for Google Drive file/folder.
        
        Returns:
            tuple: (name, mime_type, size, files, folders)
        """
        try:
            file_id = self.get_id_from_url(link, user_id)
            self.service = self.authorize()
            
            try:
                meta = self.get_file_metadata(file_id)
                name = meta.get("name")
                mime_type = meta.get("mimeType")
                
                if mime_type == self.G_DRIVE_DIR_MIME_TYPE:
                    # It's a folder - count files recursively
                    self.total_files = 0
                    self.total_folders = 0
                    size = self._count_folder(file_id)
                    return name, mime_type, size, self.total_files, self.total_folders
                else:
                    # It's a file
                    size = int(meta.get("size", 0))
                    return name, mime_type, size, 1, 0
                    
            except Exception as e:
                LOGGER.error(f"Error getting file metadata: {e}")
                return str(e), None, 0, 0, 0
                
        except Exception as e:
            LOGGER.error(f"Error in count: {e}")
            error_msg = str(e).replace(">", "").replace("<", "")
            return error_msg, None, 0, 0, 0
    
    def _count_folder(self, folder_id):
        """Recursively count files and size in a folder."""
        total_size = 0
        
        try:
            files = self.get_files_by_folder_id(folder_id)
            
            for file in files:
                shortcut_details = file.get("shortcutDetails")
                if shortcut_details:
                    file_id = shortcut_details["targetId"]
                    mime_type = shortcut_details["targetMimeType"]
                else:
                    file_id = file.get("id")
                    mime_type = file.get("mimeType")
                
                if mime_type == self.G_DRIVE_DIR_MIME_TYPE:
                    self.total_folders += 1
                    total_size += self._count_folder(file_id)
                else:
                    self.total_files += 1
                    total_size += int(file.get("size", 0))
                    
        except Exception as e:
            LOGGER.error(f"Error counting folder {folder_id}: {e}")
        
        return total_size
