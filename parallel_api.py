import asyncio
import concurrent.futures
from typing import List, Dict, Any, Callable, Optional
from functools import wraps
import time
import aiohttp
from urllib.parse import urlencode

class ParallelAPIProcessor:
    """Handle parallel API operations with direct Google Drive URLs and optimized streaming"""
    
    def __init__(self, max_workers: int = 30):
        self.max_workers = max_workers
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.session = None
    
    async def get_session(self):
        """Get or create aiohttp session for streaming"""
        if not self.session:
            connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)
            timeout = aiohttp.ClientTimeout(total=300)
            self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self.session
    
    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None
    
    async def run_parallel_tasks(self, tasks: List[Callable], *args, **kwargs) -> List[Any]:
        """Run multiple tasks in parallel with improved concurrency"""
        semaphore = asyncio.Semaphore(self.max_workers)
        
        async def limited_task(task):
            async with semaphore:
                return await task(*args, **kwargs)
        
        return await asyncio.gather(*[limited_task(task) for task in tasks], return_exceptions=True)
    
    async def batch_file_operations(self, operations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process multiple file operations concurrently with streaming"""
        semaphore = asyncio.Semaphore(min(len(operations), self.max_workers))
        
        async def process_operation(op):
            async with semaphore:
                try:
                    start_time = time.time()
                    result = await self._execute_operation(op)
                    end_time = time.time()
                    return {
                        'success': True,
                        'result': result,
                        'operation': op['type'],
                        'duration': end_time - start_time,
                        'file_id': op.get('file_id'),
                        'file_name': op.get('file_name')
                    }
                except Exception as e:
                    return {
                        'success': False,
                        'error': str(e),
                        'operation': op['type'],
                        'file_id': op.get('file_id'),
                        'file_name': op.get('file_name')
                    }
        
        tasks = [process_operation(op) for op in operations]
        return await asyncio.gather(*tasks)
    
    async def _execute_operation(self, operation: Dict[str, Any]) -> Any:
        """Execute a single operation with direct URLs"""
        op_type = operation['type']
        
        if op_type == 'get_direct_url':
            return await self._get_direct_url(operation)
        elif op_type == 'stream_download':
            return await self._stream_download(operation)
        elif op_type == 'batch_metadata':
            return await self._batch_metadata(operation)
        elif op_type == 'parallel_upload':
            return await self._parallel_upload(operation)
        elif op_type == 'parallel_delete':
            return await self._parallel_delete(operation)
        else:
            raise ValueError(f"Unknown operation type: {op_type}")
    
    async def _get_direct_url(self, operation: Dict[str, Any]) -> Dict[str, Any]:
        """Get direct Google Drive URL for file"""
        file_id = operation['file_id']
        service = operation['service']
        
        try:
            # Get file metadata with webContentLink
            file_metadata = service.files().get(
                fileId=file_id, 
                fields='id,name,mimeType,size,webContentLink,webViewLink'
            ).execute()
            
            # Generate direct download URL
            direct_url = f"https://drive.google.com/uc?export=download&id={file_id}"
            
            return {
                'file_id': file_id,
                'direct_url': direct_url,
                'web_content_link': file_metadata.get('webContentLink'),
                'web_view_link': file_metadata.get('webViewLink'),
                'name': file_metadata.get('name'),
                'size': file_metadata.get('size'),
                'mime_type': file_metadata.get('mimeType')
            }
        except Exception as e:
            raise Exception(f"Failed to get direct URL for {file_id}: {str(e)}")
    
    async def _stream_download(self, operation: Dict[str, Any]) -> Dict[str, Any]:
        """Stream download from Google Drive with optimized chunking"""
        file_id = operation['file_id']
        service = operation['service']
        
        try:
            # Get file metadata for optimal chunking
            file_metadata = service.files().get(fileId=file_id, fields='id,name,size').execute()
            file_size = int(file_metadata.get('size', 0))
            
            # Calculate optimal chunk size
            if file_size > 500 * 1024 * 1024:  # >500MB
                chunk_size = 16 * 1024 * 1024  # 16MB
            elif file_size > 100 * 1024 * 1024:  # >100MB
                chunk_size = 8 * 1024 * 1024  # 8MB
            elif file_size > 50 * 1024 * 1024:  # >50MB
                chunk_size = 4 * 1024 * 1024  # 4MB
            else:
                chunk_size = 2 * 1024 * 1024  # 2MB
            
            # Get download request
            request = service.files().get_media(fileId=file_id)
            
            return {
                'file_id': file_id,
                'stream_ready': True,
                'download_request': request,
                'optimal_chunk_size': chunk_size,
                'file_size': file_size,
                'file_name': file_metadata.get('name')
            }
        except Exception as e:
            raise Exception(f"Failed to setup optimized stream for {file_id}: {str(e)}")
    
    async def _batch_metadata(self, operation: Dict[str, Any]) -> Dict[str, Any]:
        """Get metadata for multiple files in batch"""
        file_ids = operation['file_ids']
        service = operation['service']
        
        try:
            metadata_list = []
            
            # Process in chunks to avoid API limits
            chunk_size = 10
            for i in range(0, len(file_ids), chunk_size):
                chunk = file_ids[i:i + chunk_size]
                
                # Create batch request
                batch_requests = []
                for file_id in chunk:
                    batch_requests.append(
                        service.files().get(
                            fileId=file_id,
                            fields='id,name,mimeType,size,createdTime,webContentLink,webViewLink'
                        )
                    )
                
                # Execute batch
                for request in batch_requests:
                    try:
                        metadata = request.execute()
                        metadata_list.append(metadata)
                    except Exception as e:
                        print(f"Failed to get metadata for file: {str(e)}")
            
            return {
                'metadata_list': metadata_list,
                'total_files': len(metadata_list)
            }
        except Exception as e:
            raise Exception(f"Batch metadata failed: {str(e)}")
    
    async def _parallel_upload(self, operation: Dict[str, Any]) -> Dict[str, Any]:
        """Handle parallel file upload with optimized chunking"""
        file_data = operation['file_data']
        service = operation['service']
        metadata = operation['metadata']
        
        try:
            from googleapiclient.http import MediaIoBaseUpload
            import io
            
            # Calculate optimal chunk size based on file size
            file_size = len(file_data)
            if file_size > 100 * 1024 * 1024:  # >100MB
                chunk_size = 8 * 1024 * 1024  # 8MB chunks
            elif file_size > 50 * 1024 * 1024:  # >50MB
                chunk_size = 4 * 1024 * 1024  # 4MB chunks
            elif file_size > 10 * 1024 * 1024:  # >10MB
                chunk_size = 2 * 1024 * 1024  # 2MB chunks
            else:
                chunk_size = 1024 * 1024  # 1MB default
            
            media = MediaIoBaseUpload(
                io.BytesIO(file_data),
                mimetype=metadata.get('mimeType', 'application/octet-stream'),
                resumable=True,
                chunksize=chunk_size
            )
            
            # Upload with progress tracking for large files
            request = service.files().create(
                body=metadata,
                media_body=media,
                fields='id,name,size,mimeType,createdTime,webViewLink,webContentLink'
            )
            
            response = None
            bytes_uploaded = 0
            while response is None:
                status, response = request.next_chunk()
                if status:
                    bytes_uploaded = status.resumable_progress
                    if file_size > 50 * 1024 * 1024 and bytes_uploaded % (10 * 1024 * 1024) == 0:
                        progress = (bytes_uploaded / file_size) * 100 if file_size > 0 else 0
                        print(f"Parallel upload progress: {progress:.1f}% ({bytes_uploaded}/{file_size} bytes)")
            
            return {
                'file_id': response['id'],
                'name': response['name'],
                'size': response.get('size'),
                'upload_success': True,
                'bytes_uploaded': bytes_uploaded
            }
        except Exception as e:
            raise Exception(f"Upload failed: {str(e)}")
    
    async def _parallel_delete(self, operation: Dict[str, Any]) -> Dict[str, Any]:
        """Handle parallel file deletion"""
        file_id = operation['file_id']
        service = operation['service']
        
        try:
            service.files().delete(fileId=file_id).execute()
            return {
                'file_id': file_id,
                'delete_success': True
            }
        except Exception as e:
            raise Exception(f"Delete failed for {file_id}: {str(e)}")
    
    async def get_multiple_direct_urls(self, file_ids: List[str], service) -> List[Dict[str, Any]]:
        """Get direct URLs for multiple files in parallel"""
        operations = [
            {
                'type': 'get_direct_url',
                'file_id': file_id,
                'service': service
            }
            for file_id in file_ids
        ]
        
        return await self.batch_file_operations(operations)
    
    async def stream_multiple_files(self, file_ids: List[str], service) -> List[Dict[str, Any]]:
        """Setup streaming for multiple files"""
        operations = [
            {
                'type': 'stream_download',
                'file_id': file_id,
                'service': service
            }
            for file_id in file_ids
        ]
        
        return await self.batch_file_operations(operations)

# Global instance
parallel_processor = ParallelAPIProcessor()

def async_parallel(func):
    """Decorator to make functions run in parallel"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            parallel_processor.executor, 
            func, 
            *args, 
            **kwargs
        )
    return wrapper

# Utility functions for direct URL access
async def get_direct_download_urls(file_ids: List[str], service) -> List[Dict[str, Any]]:
    """Get direct download URLs for multiple files"""
    return await parallel_processor.get_multiple_direct_urls(file_ids, service)

async def setup_streaming_downloads(file_ids: List[str], service) -> List[Dict[str, Any]]:
    """Setup streaming downloads for multiple files"""
    return await parallel_processor.stream_multiple_files(file_ids, service)

async def batch_upload_large_files(files_data: List[Dict[str, Any]], service) -> List[Dict[str, Any]]:
    """Upload multiple large files in parallel with optimized chunking"""
    operations = [
        {
            'type': 'parallel_upload',
            'file_data': file_info['data'],
            'service': service,
            'metadata': file_info['metadata']
        }
        for file_info in files_data
    ]
    
    return await parallel_processor.batch_file_operations(operations)

async def batch_download_large_files(file_ids: List[str], service) -> List[Dict[str, Any]]:
    """Setup optimized streaming for multiple large files"""
    operations = [
        {
            'type': 'stream_download',
            'file_id': file_id,
            'service': service
        }
        for file_id in file_ids
    ]
    
    return await parallel_processor.batch_file_operations(operations)
