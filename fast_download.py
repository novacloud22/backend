from fastapi import HTTPException
from googleapiclient.http import MediaIoBaseDownload
import io

def create_fast_download_stream(service, file_id, file_size):
    """Create optimized download stream for 50MB+ files"""
    
    # Use aggressive chunking for 50MB+ files
    if file_size > 50 * 1024 * 1024:
        chunk_size = 8 * 1024 * 1024  # 8MB chunks
    elif file_size > 20 * 1024 * 1024:
        chunk_size = 4 * 1024 * 1024  # 4MB chunks
    else:
        chunk_size = 2 * 1024 * 1024  # 2MB chunks
    
    def generate_fast_stream():
        request = service.files().get_media(fileId=file_id)
        file_io = io.BytesIO()
        downloader = MediaIoBaseDownload(file_io, request, chunksize=chunk_size)
        
        done = False
        total_downloaded = 0
        
        while not done:
            try:
                status, done = downloader.next_chunk()
                if status:
                    file_io.seek(0)
                    chunk_data = file_io.read()
                    if chunk_data:
                        total_downloaded += len(chunk_data)
                        yield chunk_data
                    file_io.seek(0)
                    file_io.truncate(0)
                    
                    # Progress every 5MB for 50MB+ files
                    if file_size > 50 * 1024 * 1024 and total_downloaded % (5 * 1024 * 1024) == 0:
                        progress = (total_downloaded / file_size) * 100
                        print(f"Fast download: {progress:.0f}%")
                        
            except Exception as e:
                print(f"Fast download error: {str(e)}")
                break
    
    return generate_fast_stream()
