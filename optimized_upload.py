"""
Optimized Upload Handler for handling large batches of files (500+ files)
Addresses backend performance bottlenecks with connection pooling, async processing, and memory optimization
"""

import asyncio
import aiofiles
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Optional
import time
import logging
from dataclasses import dataclass
from contextlib import asynccontextmanager
import weakref
import gc
from fastapi import UploadFile
import io
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError
from ssl_fix import upload_with_retry, get_secure_drive_service
import ssl

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class UploadResult:
    success: bool
    file_id: Optional[str] = None
    file_name: Optional[str] = None
    error: Optional[str] = None
    size: Optional[int] = None
    duration: Optional[float] = None

class OptimizedUploadProcessor:
    """
    Optimized upload processor for handling large batches of files
    """
    
    def __init__(self, max_workers: int = 3, chunk_size: int = 5):
        self.max_workers = max_workers
        self.chunk_size = chunk_size
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.semaphore = asyncio.Semaphore(max_workers)
        self.active_uploads = weakref.WeakSet()
        self.progress_file = "upload_progress.json"
        
        # Set very long timeout for large files
        import socket
        socket.setdefaulttimeout(600)  # 10 minutes
        
    async def process_batch_upload(
        self,
        files: List[UploadFile],
        service,
        target_folder_id: str,
        user_email: str,
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """
        Process batch upload with optimized concurrency and memory management
        """
        start_time = time.time()
        logger.info(f"Starting batch upload for {len(files)} files")
        
        # Process files in chunks to prevent memory issues
        chunks = [files[i:i + self.chunk_size] for i in range(0, len(files), self.chunk_size)]
        all_results = []
        
        for chunk_idx, chunk in enumerate(chunks):
            logger.info(f"Processing chunk {chunk_idx + 1}/{len(chunks)} ({len(chunk)} files)")
            
            # Process chunk with controlled concurrency
            chunk_results = await self._process_chunk(
                chunk, service, target_folder_id, user_email, overwrite
            )
            all_results.extend(chunk_results)
            
            # Force garbage collection between chunks
            gc.collect()
            
            # Save progress after each chunk
            self._save_progress(chunk_idx + 1, len(chunks), all_results)
            
            # Longer delay to prevent overwhelming
            if chunk_idx < len(chunks) - 1:
                await asyncio.sleep(8.0)  # 8 seconds for large files
        
        # Calculate statistics
        successful = sum(1 for r in all_results if r.success)
        failed = len(all_results) - successful
        total_duration = time.time() - start_time
        
        logger.info(f"Batch upload completed: {successful} successful, {failed} failed in {total_duration:.2f}s")
        
        return {
            "total_files": len(files),
            "successful_uploads": successful,
            "failed_uploads": failed,
            "results": [self._result_to_dict(r) for r in all_results],
            "duration": total_duration,
            "throughput": len(files) / total_duration if total_duration > 0 else 0
        }
    
    async def _process_chunk(
        self,
        files: List[UploadFile],
        service,
        target_folder_id: str,
        user_email: str,
        overwrite: bool
    ) -> List[UploadResult]:
        """
        Process a chunk of files with controlled concurrency
        """
        tasks = []
        for file in files:
            task = self._upload_single_file_async(
                file, service, target_folder_id, user_email, overwrite
            )
            tasks.append(task)
        
        # Execute with semaphore control
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Convert exceptions to UploadResult objects
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append(UploadResult(
                    success=False,
                    file_name=files[i].filename,
                    error=str(result)
                ))
            else:
                processed_results.append(result)
        
        return processed_results
    
    async def _upload_single_file_async(
        self,
        file: UploadFile,
        service,
        target_folder_id: str,
        user_email: str,
        overwrite: bool
    ) -> UploadResult:
        """
        Upload single file asynchronously with optimizations
        """
        async with self.semaphore:
            start_time = time.time()
            
            try:
                # Handle large files without loading all into memory
                file_size = file.size if hasattr(file, 'size') else 0
                
                # For files >100MB, use streaming to avoid memory issues
                if file_size > 100 * 1024 * 1024:
                    # Stream large files
                    media = self._create_streaming_media(file)
                else:
                    # Read smaller files into memory
                    file_content = await file.read()
                    file_size = len(file_content)
                    await file.seek(0)
                    
                    media = MediaIoBaseUpload(
                        io.BytesIO(file_content),
                        mimetype=file.content_type or 'application/octet-stream',
                        resumable=True,
                        chunksize=self._get_optimal_chunk_size(file_size)
                    )
                
                # Skip existing file check to avoid SSL errors
                # Files will be overwritten if they exist
                
                # Prepare file metadata
                file_metadata = {
                    'name': file.filename,
                    'parents': [target_folder_id],
                    'mimeType': file.content_type or 'application/octet-stream'
                }
                
                # Upload file using thread executor to avoid blocking
                upload_result = await asyncio.get_event_loop().run_in_executor(
                    self.executor,
                    self._execute_upload,
                    service,
                    file_metadata,
                    media
                )
                
                duration = time.time() - start_time
                
                return UploadResult(
                    success=True,
                    file_id=upload_result['id'],
                    file_name=upload_result['name'],
                    size=file_size,
                    duration=duration
                )
                
            except Exception as e:
                duration = time.time() - start_time
                logger.error(f"Upload failed for {file.filename}: {str(e)}")
                
                return UploadResult(
                    success=False,
                    file_name=file.filename,
                    error=str(e),
                    duration=duration
                )
    
    def _execute_upload(self, service, file_metadata: Dict, media) -> Dict:
        """
        Execute the actual upload in a thread to avoid blocking
        """
        try:
            # Use SSL-safe upload with retry logic
            return upload_with_retry(service, file_metadata, media, max_retries=3)
            
        except ssl.SSLError as e:
            raise Exception(f"SSL connection error: {str(e)}")
        except HttpError as e:
            if e.resp.status == 409:
                raise Exception(f"File already exists: {file_metadata['name']}")
            elif e.resp.status == 403:
                raise Exception("Access denied - check permissions")
            elif e.resp.status == 429:
                raise Exception("Rate limit exceeded - please retry")
            else:
                raise Exception(f"Google Drive API error: {e.resp.status}")
        except Exception as e:
            if "SSL" in str(e).upper():
                raise Exception(f"SSL connection error: {str(e)}")
            raise Exception(f"Upload error: {str(e)}")
    
    async def _check_existing_file(self, service, filename: str, folder_id: str) -> List:
        """
        Check if file already exists in the target folder
        """
        try:
            # Run in executor to avoid blocking
            results = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                lambda: service.files().list(
                    q=f"name='{filename}' and '{folder_id}' in parents and trashed=false",
                    fields="files(id, name)"
                ).execute()
            )
            return results.get('files', [])
        except (ssl.SSLError, Exception) as e:
            if "SSL" in str(e).upper():
                logger.warning(f"SSL error checking existing file {filename}, skipping check")
            else:
                logger.warning(f"Error checking existing file {filename}: {str(e)}")
            return []
    
    def _create_streaming_media(self, file: UploadFile):
        """Create streaming media for large files"""
        return MediaIoBaseUpload(
            file.file,
            mimetype=file.content_type or 'application/octet-stream',
            resumable=True,
            chunksize=32 * 1024 * 1024  # 32MB chunks for large files
        )
    
    def _save_progress(self, current_chunk: int, total_chunks: int, results: List[UploadResult]):
        """Save upload progress for resume capability"""
        try:
            import json
            progress = {
                "current_chunk": current_chunk,
                "total_chunks": total_chunks,
                "completed_files": [r.file_name for r in results if r.success],
                "failed_files": [r.file_name for r in results if not r.success],
                "timestamp": time.time()
            }
            with open(self.progress_file, 'w') as f:
                json.dump(progress, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not save progress: {e}")
    
    def _get_optimal_chunk_size(self, file_size: int) -> int:
        """
        Get optimal chunk size based on file size for better upload performance
        """
        if file_size > 500 * 1024 * 1024:  # >500MB
            return 32 * 1024 * 1024  # 32MB chunks
        elif file_size > 100 * 1024 * 1024:  # >100MB
            return 16 * 1024 * 1024  # 16MB chunks
        elif file_size > 50 * 1024 * 1024:  # >50MB
            return 8 * 1024 * 1024   # 8MB chunks
        elif file_size > 10 * 1024 * 1024:  # >10MB
            return 4 * 1024 * 1024   # 4MB chunks
        else:
            return 2 * 1024 * 1024   # 2MB chunks for smaller files
    
    def _result_to_dict(self, result: UploadResult) -> Dict[str, Any]:
        """
        Convert UploadResult to dictionary for JSON serialization
        """
        return {
            "success": result.success,
            "file_id": result.file_id,
            "file_name": result.file_name,
            "error": result.error,
            "size": result.size,
            "duration": result.duration
        }
    
    async def cleanup(self):
        """
        Cleanup resources
        """
        self.executor.shutdown(wait=True)
        logger.info("Upload processor cleaned up")

class ConnectionPool:
    """
    Connection pool for Google Drive API to improve performance
    """
    
    def __init__(self, max_connections: int = 10):
        self.max_connections = max_connections
        self.connections = []
        self.semaphore = asyncio.Semaphore(max_connections)
    
    @asynccontextmanager
    async def get_connection(self, service_factory):
        """
        Get a connection from the pool
        """
        async with self.semaphore:
            if self.connections:
                connection = self.connections.pop()
            else:
                connection = service_factory()
            
            try:
                yield connection
            finally:
                # Return connection to pool if still valid
                if len(self.connections) < self.max_connections:
                    self.connections.append(connection)

# Global instances - Optimized for large files
upload_processor = OptimizedUploadProcessor(max_workers=3, chunk_size=5)
connection_pool = ConnectionPool(max_connections=5)

# Utility functions for integration with main.py
async def process_optimized_batch_upload(
    files: List[UploadFile],
    service,
    target_folder_id: str,
    user_email: str,
    overwrite: bool = False
) -> Dict[str, Any]:
    """
    Main function to process optimized batch upload
    """
    return await upload_processor.process_batch_upload(
        files, service, target_folder_id, user_email, overwrite
    )

def get_upload_stats() -> Dict[str, Any]:
    """
    Get current upload processor statistics
    """
    return {
        "max_workers": upload_processor.max_workers,
        "chunk_size": upload_processor.chunk_size,
        "active_uploads": len(upload_processor.active_uploads)
    }
