from fastapi import FastAPI, HTTPException, Depends, UploadFile, File, Form, WebSocket, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, StreamingResponse, HTMLResponse
from fastapi.websockets import WebSocketDisconnect
from pydantic import BaseModel
from typing import Optional, List
import os
import json
import io
import uuid
import asyncio
from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, auth, firestore
import pyotp
import qrcode
import base64
from io import BytesIO
import secrets
import hashlib
import threading

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request as GoogleRequest
from dotenv import load_dotenv
from parallel_api import parallel_processor, async_parallel

load_dotenv()

# Dynamic chunk sizing for better performance
def get_optimal_chunk_size(file_size: int) -> int:
    """Get optimal chunk size based on file size for faster downloads"""
    if file_size > 500 * 1024 * 1024:  # >500MB
        return 8 * 1024 * 1024  # 8MB
    elif file_size > 100 * 1024 * 1024:  # >100MB
        return 4 * 1024 * 1024  # 4MB
    elif file_size > 10 * 1024 * 1024:  # >10MB
        return 2 * 1024 * 1024  # 2MB
    return 1024 * 1024  # 1MB default

# Connection pooling for Google API
_http_session = None
_session_lock = threading.Lock()

def get_http_session():
    global _http_session
    if _http_session is None:
        with _session_lock:
            if _http_session is None:
                _http_session = GoogleRequest()
    return _http_session

app = FastAPI(title="Novacloud API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security
security = HTTPBearer()

# Firebase Admin SDK initialization
try:
    if not firebase_admin._apps:
        firebase_creds_json = os.getenv('FIREBASE_SERVICE_ACCOUNT_JSON')
        if firebase_creds_json:
            cred = credentials.Certificate(json.loads(firebase_creds_json))
        else:
            cred = credentials.Certificate('./firebase-service-account.json')
        firebase_admin.initialize_app(cred)
    db = firestore.client()
except Exception as e:
    print(f"Firebase initialization failed: {str(e)}")
    db = None

# Google OAuth settings
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
GOOGLE_REDIRECT_URI = os.getenv("GOOGLE_REDIRECT_URI")

# Local file storage (legacy)
TOKENS_FILE = "tokens.json"
SESSIONS_FILE = "sessions.json"
MAX_DRIVE_CONNECTIONS = 5

def load_json_file(filename, default=None):
    if default is None:
        default = {}
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return default

def save_json_file(filename, data):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)

# Legacy JSON loading (will be removed)
user_tokens = load_json_file(TOKENS_FILE)
user_sessions = load_json_file(SESSIONS_FILE)
# Test Firestore connection on startup
if db:
    try:
        test_doc = db.collection('startup_test').document('backend_init')
        test_doc.set({'timestamp': datetime.utcnow().isoformat(), 'status': 'backend_started'})
        print("✓ Firestore connection successful")
    except Exception as e:
        print(f"✗ Firestore connection failed: {str(e)}")
        print("Warning: User data will not be saved/retrieved properly")
else:
    print("✗ Firestore not initialized")
active_connections = {}

# Pydantic models
class UserCreate(BaseModel):
    email: str
    name: str
    uid: str
    phone: Optional[str] = None
    organization: Optional[str] = None

class UserProfile(BaseModel):
    email: str
    name: str
    phone: Optional[str] = None
    organization: Optional[str] = None
    created_at: str
    storage_used: int
    total_files: int
    total_folders: int
    last_login: Optional[str] = None
    google_connected: bool
    personal_drive_connected: bool = False

class Token(BaseModel):
    access_token: str
    token_type: str

class FileInfo(BaseModel):
    id: str
    name: str
    size: int
    mimeType: str
    createdTime: str
    webViewLink: Optional[str] = None
    webContentLink: Optional[str] = None
    path: Optional[str] = None
    basename: Optional[str] = None

class TwoFASetup(BaseModel):
    secret: str
    qr_code: str
    backup_codes: List[str]

class TwoFAVerify(BaseModel):
    token: str

class TwoFAStatus(BaseModel):
    enabled: bool
    backup_codes_remaining: int

class TwoFASetup(BaseModel):
    secret: str
    qr_code: str
    backup_codes: List[str]

class TwoFAVerify(BaseModel):
    token: str

class TwoFAStatus(BaseModel):
    enabled: bool
    backup_codes_remaining: int

# Utility functions

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        if not credentials:
            raise HTTPException(status_code=401, detail="No authentication credentials provided")
        
        # Verify Firebase ID token
        decoded_token = auth.verify_id_token(credentials.credentials)
        email = decoded_token.get('email')
        uid = decoded_token.get('uid')
        
        if not email:
            raise HTTPException(status_code=401, detail="Email not found in token")
        
        print(f"Authenticated user: {email} (UID: {uid})")
        
        # Ensure user exists in Firestore
        user_data = get_user_from_firestore(email)
        if not user_data:
            print(f"User {email} not found in Firestore, creating record")
            # Auto-create user record if missing
            try:
                firebase_user = auth.get_user(uid)
                new_user = {
                    "email": email,
                    "name": firebase_user.display_name or "User",
                    "uid": uid,
                    "created_at": datetime.utcnow().isoformat(),
                    "storage_used": 0,
                    "total_files": 0,
                    "total_folders": 0,
                    "last_login": datetime.utcnow().isoformat(),
                    "folder_id": None
                }
                save_user_to_firestore(email, new_user)
                print(f"Auto-created user record for {email}")
            except Exception as create_error:
                print(f"Failed to auto-create user record: {str(create_error)}")
        
        return email
    except Exception as e:
        print(f"Auth Error: {str(e)}")
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")

def get_optional_current_user(request: Request):
    """Custom optional auth that handles null tokens gracefully"""
    try:
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return None
            
        token = auth_header[7:]  # Remove 'Bearer ' prefix
        
        # Handle null/undefined tokens
        if not token or token in ['null', 'undefined', '', 'None']:
            return None
            
        decoded_token = auth.verify_id_token(token)
        return decoded_token.get('email')
    except Exception:
        return None

def get_google_service():
    shared_tokens = load_json_file('shared_tokens.json')
    if not shared_tokens:
        return None
    
    try:
        creds = Credentials.from_authorized_user_info(shared_tokens)
        if not creds.valid:
            if creds.expired and creds.refresh_token:
                from google.auth.transport.requests import Request
                creds.refresh(Request())
                # Save refreshed token
                save_json_file('shared_tokens.json', json.loads(creds.to_json()))
            else:
                return None
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        print(f"Error getting Google service: {str(e)}")
        return None

def get_user_drive_tokens_from_firestore(user_email: str):
    """Get user drive tokens from Firestore"""
    if not db:
        return {}
    try:
        doc_ref = db.collection('user_drive_tokens').document(user_email)
        doc = doc_ref.get()
        if doc.exists:
            return doc.to_dict()
        return {}
    except Exception as e:
        print(f"Error getting tokens from Firestore: {str(e)}")
        return {}

def save_user_drive_tokens_to_firestore(user_email: str, tokens_data: dict):
    """Save user drive tokens to Firestore"""
    if not db:
        print(f"Firestore not available, cannot save tokens for {user_email}")
        return
    try:
        doc_ref = db.collection('user_drive_tokens').document(user_email)
        doc_ref.set(tokens_data)
        print(f"Saved tokens to Firestore for {user_email}")
    except Exception as e:
        print(f"Error saving tokens to Firestore: {str(e)}")

# Firestore user management functions
def get_user_from_firestore(user_email: str):
    """Get user data from Firestore"""
    if not db:
        return None
    try:
        doc_ref = db.collection('users').document(user_email)
        doc = doc_ref.get()
        return doc.to_dict() if doc.exists else None
    except Exception as e:
        print(f"Error getting user from Firestore: {str(e)}")
        return None

def save_user_to_firestore(user_email: str, user_data: dict, overwrite: bool = False):
    """Save user data to Firestore"""
    if not db:
        print(f"Firestore not available, cannot save user {user_email}")
        return False
    try:
        doc_ref = db.collection('users').document(user_email)
        if overwrite:
            # Completely overwrite existing data
            doc_ref.set(user_data)
            print(f"Successfully overwritten user {user_email} in Firestore")
        else:
            # Merge with existing data
            doc_ref.set(user_data, merge=True)
            print(f"Successfully saved user {user_email} to Firestore")
        return True
    except Exception as e:
        print(f"Error saving user to Firestore: {str(e)}")
        return False

def update_user_in_firestore(user_email: str, updates: dict):
    """Update specific fields for a user in Firestore"""
    if not db:
        return False
    try:
        doc_ref = db.collection('users').document(user_email)
        doc_ref.update(updates)
        return True
    except Exception as e:
        print(f"Error updating user in Firestore: {str(e)}")
        return False

def check_user_exists_in_firestore(user_email: str):
    """Check if user exists in Firestore"""
    if not db:
        return False
    try:
        doc_ref = db.collection('users').document(user_email)
        doc = doc_ref.get()
        exists = doc.exists
        print(f"Firestore check for {user_email}: {exists}")
        return exists
    except Exception as e:
        print(f"Error checking user existence: {str(e)}")
        return False

def get_all_users_from_firestore():
    """Get all users from Firestore (for statistics)"""
    if not db:
        return []
    try:
        users_ref = db.collection('users')
        docs = users_ref.stream()
        return [doc.to_dict() for doc in docs]
    except Exception as e:
        print(f"Error getting all users: {str(e)}")
        return []

def cleanup_user_data(user_email: str):
    """Simple and reliable cleanup of all user data from Firestore"""
    if not db:
        print("Firestore not available")
        return False
    
    cleanup_results = {}
    collections_to_clean = [
        'users',
        'user_2fa', 
        'user_drive_tokens',
        'email_change_requests'
    ]
    
    # Use batch delete for better reliability
    batch = db.batch()
    
    # Clean up regular collections
    for collection_name in collections_to_clean:
        try:
            doc_ref = db.collection(collection_name).document(user_email)
            batch.delete(doc_ref)
            cleanup_results[collection_name] = "queued_for_deletion"
            print(f"Queued {collection_name} for deletion: {user_email}")
        except Exception as e:
            cleanup_results[collection_name] = f"error: {str(e)}"
            print(f"Error queuing {collection_name} for deletion: {str(e)}")
    
    # Get and queue share links for deletion
    try:
        shares_ref = db.collection('share_links')
        query = shares_ref.where('owner_email', '==', user_email)
        docs = list(query.stream())
        
        for doc in docs:
            batch.delete(doc.reference)
        
        cleanup_results['share_links'] = f"queued_{len(docs)}_for_deletion"
        print(f"Queued {len(docs)} share links for deletion")
        
    except Exception as e:
        cleanup_results['share_links'] = f"error: {str(e)}"
        print(f"Error queuing share links for deletion: {str(e)}")
    
    # Execute batch delete
    try:
        batch.commit()
        print(f"✓ Batch deletion committed for {user_email}")
        
        # Update results to reflect successful deletion
        for key in cleanup_results:
            if "queued" in cleanup_results[key]:
                cleanup_results[key] = cleanup_results[key].replace("queued", "deleted")
        
    except Exception as e:
        print(f"✗ Batch deletion failed: {str(e)}")
        for key in cleanup_results:
            if "queued" in cleanup_results[key]:
                cleanup_results[key] = f"batch_error: {str(e)}"
    
    return cleanup_results



def get_user_google_service(user_email: str, drive_id: str = "drive_1"):
    """Get Google Drive service for a specific user's personal account"""
    # Get tokens from Firestore
    user_tokens = get_user_drive_tokens_from_firestore(user_email)
    
    if drive_id not in user_tokens:
        print(f"No tokens found for user {user_email}, drive {drive_id}")
        return None
    
    try:
        token_data = user_tokens[drive_id]
        print(f"Found token data for {user_email} - {drive_id}")
        
        creds = Credentials.from_authorized_user_info(token_data)
        if not creds.valid:
            if creds.expired and creds.refresh_token:
                print(f"Refreshing expired token for {user_email} - {drive_id}")
                from google.auth.transport.requests import Request
                creds.refresh(Request())
                # Save refreshed token to Firestore
                user_tokens[drive_id] = json.loads(creds.to_json())
                save_user_drive_tokens_to_firestore(user_email, user_tokens)
            else:
                print(f"Invalid credentials for {user_email} - {drive_id}")
                return None
        
        service = build('drive', 'v3', credentials=creds)
        print(f"Successfully created service for {user_email} - {drive_id}")
        
        # Quick test to ensure service works
        try:
            service.about().get(fields="user").execute()
            print(f"Service test passed for {user_email} - {drive_id}")
        except Exception as test_e:
            print(f"Service test failed for {user_email} - {drive_id}: {str(test_e)}")
            return None
            
        return service
        
    except Exception as e:
        print(f"Error getting user Google service for {user_email} - {drive_id}: {str(e)}")
        return None

def get_user_folder(service, user_email: str):
    folder_name = user_email.replace('@', '_at_').replace('.', '_')
    
    try:
        # Check if folder exists
        results = service.files().list(
            q=f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false",
            fields="files(id, name)"
        ).execute()
        
        folders = results.get('files', [])
        if folders:
            return folders[0]['id']
        
        # Create folder if it doesn't exist
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        folder = service.files().create(body=folder_metadata, fields='id').execute()
        return folder.get('id')
    except Exception as e:
        print(f"Error in get_user_folder: {str(e)}")
        return None

def create_user_folder(user_email: str):
    service = get_google_service()
    if not service:
        return None
    try:
        return get_user_folder(service, user_email)
    except:
        return None



def update_user_storage(user_email: str):
    """Update user's storage usage from shared drive"""
    user_data = get_user_from_firestore(user_email)
    if not user_data:
        return
    
    service = get_google_service()
    if not service:
        return
    
    user_folder_id = user_data.get('folder_id')
    if not user_folder_id:
        return
    
    try:
        total_size = 0
        results = service.files().list(
            q=f"'{user_folder_id}' in parents and trashed=false",
            fields="files(size,mimeType)"
        ).execute()
        
        for file in results.get('files', []):
            if file.get('mimeType') != 'application/vnd.google-apps.folder':
                total_size += int(file.get('size', 0))
        
        # Update storage in Firestore
        update_user_in_firestore(user_email, {'storage_used': total_size})
        
        # Send WebSocket update
        if user_email in active_connections:
            try:
                def format_size(size_bytes):
                    if size_bytes == 0:
                        return "0 B"
                    size_names = ["B", "KB", "MB", "GB", "TB"]
                    import math
                    i = int(math.floor(math.log(size_bytes, 1024)))
                    p = math.pow(1024, i)
                    s = round(size_bytes / p, 2)
                    if s == int(s):
                        s = int(s)
                    return f"{s} {size_names[i]}"
                
                storage_data = {
                    "storage_used": total_size,
                    "storage_display": format_size(total_size),
                    "storage_limit": 5368709120,
                    "limit_display": "5 GB",
                    "storage_percentage": (total_size / 5368709120 * 100) if total_size > 0 else 0
                }
                
                import asyncio
                asyncio.create_task(active_connections[user_email].send_json({
                    'type': 'storage_update',
                    'data': storage_data
                }))
            except:
                pass
        
    except Exception as e:
        print(f"Error updating storage for {user_email}: {str(e)}")

# Routes
# Deployment trigger - v2.1 with share endpoints
@app.get("/")
async def root():
    return {"message": "Novacloud API - Made in India 🇮🇳 - v2.1"}

@app.get("/test-share")
async def test_share_endpoint():
    return {"message": "Share endpoints are working"}

@app.get("/health")
async def health_check():
    """Health check endpoint to verify services"""
    shared_service = get_google_service()
    
    # Test Firestore connection
    firestore_working = False
    if db:
        try:
            test_doc = db.collection('health_check').document('test')
            test_doc.set({'timestamp': datetime.utcnow().isoformat()})
            firestore_working = True
        except Exception as e:
            print(f"Firestore health check failed: {str(e)}")
    
    return {
        "status": "healthy",
        "shared_drive_connected": shared_service is not None,
        "firestore_connected": firestore_working,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/check-user-exists")
async def check_user_exists(email: str):
    """Check if user exists in database"""
    # Check Firebase Auth first (primary source of truth)
    firebase_exists = False
    try:
        auth.get_user_by_email(email)
        firebase_exists = True
    except:
        firebase_exists = False
    
    # If user doesn't exist in Firebase Auth, they don't exist (even if orphaned data in Firestore)
    if not firebase_exists:
        return {"exists": False}
    
    # If user exists in Firebase Auth, check Firestore
    firestore_exists = check_user_exists_in_firestore(email)
    
    return {
        "exists": firebase_exists,
        "firestore_exists": firestore_exists,
        "firebase_exists": firebase_exists
    }

@app.post("/register-user")
async def register_user(user_data: UserCreate):
    """Register a new user in the database"""
    print(f"Registering user: {user_data.email} with UID: {user_data.uid}")
    
    # Check if user already exists in Firestore
    existing_user = get_user_from_firestore(user_data.email)
    if existing_user:
        existing_uid = existing_user.get('uid')
        print(f"Found existing user {user_data.email} with UID: {existing_uid}")
        
        # If UID is different, this is a new Firebase user with same email (after deletion)
        if existing_uid != user_data.uid:
            print(f"UID mismatch - old: {existing_uid}, new: {user_data.uid}. Overwriting with new user data.")
            
            # Clean up any orphaned data first
            cleanup_user_data(user_data.email)
            
            # Create completely new user record
            new_user = {
                "email": user_data.email,
                "name": user_data.name,
                "uid": user_data.uid,
                "phone": user_data.phone,
                "organization": user_data.organization,
                "created_at": datetime.utcnow().isoformat(),
                "storage_used": 0,
                "total_files": 0,
                "total_folders": 0,
                "last_login": datetime.utcnow().isoformat(),
                "folder_id": None
            }
            
            # Force overwrite existing data
            success = save_user_to_firestore(user_data.email, new_user, overwrite=True)
            if not success:
                raise HTTPException(status_code=500, detail="Failed to register user")
            
            print(f"Successfully overwritten user data for {user_data.email}")
            return {"message": "User registered successfully"}
        else:
            # Same UID, just update login time
            print(f"Same UID, updating login time for {user_data.email}")
            update_user_in_firestore(user_data.email, {
                "last_login": datetime.utcnow().isoformat()
            })
            return {"message": "User login successful"}
    
    # Create new user (no existing record)
    print(f"Creating new user record for {user_data.email}")
    new_user = {
        "email": user_data.email,
        "name": user_data.name,
        "uid": user_data.uid,
        "phone": user_data.phone,
        "organization": user_data.organization,
        "created_at": datetime.utcnow().isoformat(),
        "storage_used": 0,
        "total_files": 0,
        "total_folders": 0,
        "last_login": datetime.utcnow().isoformat(),
        "folder_id": None
    }
    
    success = save_user_to_firestore(user_data.email, new_user)
    if not success:
        print(f"Failed to save user {user_data.email} to Firestore")
        raise HTTPException(status_code=500, detail="Failed to register user")
    
    print(f"Successfully registered user {user_data.email}")
    return {"message": "User registered successfully"}



@app.get("/setup-drive")
async def setup_drive():
    try:
        flow = Flow.from_client_secrets_file(
            'credentials.json',
            scopes=[
                'https://www.googleapis.com/auth/drive.file',
                'https://www.googleapis.com/auth/userinfo.profile',
                'https://www.googleapis.com/auth/userinfo.email',
                'openid'
            ]
        )
        flow.redirect_uri = os.getenv('GOOGLE_REDIRECT_URI', 'https://backend-vjzu.onrender.com/oauth2callback')
        
        authorization_url, state = flow.authorization_url(
            access_type='offline',
            prompt='consent',
            include_granted_scopes='true'
        )
        
        return {"authorization_url": authorization_url}
        
    except Exception as e:
        print(f"Error in setup_drive: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to setup drive: {str(e)}")

@app.get("/connect-personal-drive")
async def connect_personal_drive(drive_id: str = "drive_1", current_user: str = Depends(get_current_user)):
    """Generate authorization URL for user to connect their personal Google Drive"""
    # Validate drive_id format
    if not drive_id.startswith('drive_') or not drive_id.split('_')[1].isdigit():
        raise HTTPException(status_code=400, detail="Invalid drive_id format. Use drive_1, drive_2, etc.")
    
    drive_num = int(drive_id.split('_')[1])
    if drive_num < 1 or drive_num > MAX_DRIVE_CONNECTIONS:
        raise HTTPException(status_code=400, detail=f"Drive number must be between 1 and {MAX_DRIVE_CONNECTIONS}")
    
    try:
        flow = Flow.from_client_secrets_file(
            'credentials.json',
            scopes=[
                'https://www.googleapis.com/auth/drive.file',
                'https://www.googleapis.com/auth/userinfo.profile',
                'https://www.googleapis.com/auth/userinfo.email',
                'openid'
            ]
        )
        flow.redirect_uri = os.getenv('GOOGLE_REDIRECT_URI', 'https://backend-vjzu.onrender.com/oauth2callback')
        
        state_value = f'user:{current_user}:drive:{drive_id}'
        print(f"Generating auth URL with state: {state_value}")
        
        authorization_url, state = flow.authorization_url(
            access_type='offline',
            prompt='consent',
            include_granted_scopes='true',
            state=state_value
        )
        
        print(f"Generated authorization URL: {authorization_url}")
        
        return {"authorization_url": authorization_url}
        
    except Exception as e:
        print(f"Error generating auth URL: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to generate authorization URL: {str(e)}")

@app.post("/disconnect-personal-drive")
async def disconnect_personal_drive(drive_id: str = "drive_1", current_user: str = Depends(get_current_user)):
    """Disconnect user's personal Google Drive"""
    user_tokens = get_user_drive_tokens_from_firestore(current_user)
    if drive_id in user_tokens:
        del user_tokens[drive_id]
        if user_tokens:
            save_user_drive_tokens_to_firestore(current_user, user_tokens)
        elif db:
            # Delete document if no drives left
            try:
                db.collection('user_drive_tokens').document(current_user).delete()
            except Exception as e:
                print(f"Error deleting tokens: {str(e)}")
    
    return {"message": f"Google Drive {drive_id} disconnected successfully"}

@app.get("/personal-drive-status")
async def get_personal_drive_status(current_user: str = Depends(get_current_user)):
    """Check status of all connected personal Google Drives"""
    user_drives = get_user_drive_tokens_from_firestore(current_user)
    drives_status = {}
    
    print(f"Checking drive status for user: {current_user}")
    print(f"Available drives in tokens: {list(user_drives.keys())}")
    
    for drive_id in [f"drive_{i}" for i in range(1, MAX_DRIVE_CONNECTIONS + 1)]:
        if drive_id in user_drives:
            print(f"Checking {drive_id} - has tokens")
            service = get_user_google_service(current_user, drive_id)
            if service:
                try:
                    about = service.about().get(fields="user").execute()
                    email = about.get('user', {}).get('emailAddress')
                    print(f"{drive_id} connected to: {email}")
                    drives_status[drive_id] = {
                        "connected": True,
                        "email": email
                    }
                except Exception as e:
                    print(f"Error getting drive email for {drive_id}: {str(e)}")
                    drives_status[drive_id] = {
                        "connected": False,
                        "email": None,
                        "error": str(e)
                    }
            else:
                print(f"{drive_id} - service creation failed")
                drives_status[drive_id] = {
                    "connected": False,
                    "email": None,
                    "error": "Service creation failed"
                }
        else:
            drives_status[drive_id] = {
                "connected": False,
                "email": None
            }
    
    total_connected = sum(1 for status in drives_status.values() if status["connected"])
    print(f"Total connected drives: {total_connected}")
    
    return {
        "drives": drives_status,
        "total_connected": total_connected,
        "max_connections": MAX_DRIVE_CONNECTIONS
    }

@app.get("/oauth2callback")
async def oauth2callback(code: Optional[str] = None, state: Optional[str] = None):
    frontend_url = os.getenv('FRONTEND_URL', 'https://novacloud22.web.app')
    
    if not code:
        return {"error": "Missing authorization code. This endpoint should only be accessed by Google OAuth."}
    
    try:
        flow = Flow.from_client_config(
            {
                "web": {
                    "client_id": os.getenv('GOOGLE_CLIENT_ID'),
                    "client_secret": os.getenv('GOOGLE_CLIENT_SECRET'),
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": [os.getenv('GOOGLE_REDIRECT_URI')]
                }
            },
            scopes=[
                'https://www.googleapis.com/auth/drive.file',
                'https://www.googleapis.com/auth/userinfo.profile',
                'https://www.googleapis.com/auth/userinfo.email',
                'openid'
            ]
        )
        flow.redirect_uri = os.getenv('GOOGLE_REDIRECT_URI', 'https://backend-vjzu.onrender.com/oauth2callback')
        
        # Fetch token without duplicate redirect_uri
        flow.fetch_token(code=code)
        credentials = flow.credentials
        
        # Ensure we have the required scopes
        required_scopes = ['https://www.googleapis.com/auth/drive.file']
        if not any(scope in credentials.scopes for scope in required_scopes):
            raise Exception("Required drive.file scope not granted")
        
        # Check if this is for a personal drive connection
        if state and state.startswith('user:'):
            parts = state.split(':')
            print(f"OAuth callback - State parts: {parts}")
            
            if len(parts) >= 4 and parts[2] == 'drive':
                user_email = parts[1]
                drive_id = parts[3]
                
                print(f"Personal drive auth - User: {user_email}, Drive: {drive_id}")
                
                # Get existing tokens from Firestore
                user_tokens = get_user_drive_tokens_from_firestore(user_email)
                
                # Save user's personal drive tokens
                token_data = json.loads(credentials.to_json())
                user_tokens[drive_id] = token_data
                save_user_drive_tokens_to_firestore(user_email, user_tokens)
                    
                print(f"Successfully saved tokens for {user_email} - {drive_id}")
                
                # Test the saved tokens immediately
                test_service = get_user_google_service(user_email, drive_id)
                if test_service:
                    try:
                        about = test_service.about().get(fields="user").execute()
                        print(f"Token test successful - connected to: {about.get('user', {}).get('emailAddress')}")
                    except Exception as test_e:
                        print(f"Token test failed: {str(test_e)}")
                else:
                    print(f"Failed to create service from saved tokens")
                    
                # For popup windows, return a simple HTML page that closes the popup
                html_content = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Authorization Complete</title>
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <style>
                        body {{ font-family: Arial, sans-serif; text-align: center; padding: 20px; background: #f8f9fa; margin: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center; }}
                        .success {{ color: #28a745; margin-bottom: 20px; font-size: 24px; font-weight: bold; }}
                        .container {{ max-width: 90%; width: 400px; margin: 0 auto; background: white; padding: 40px 20px; border-radius: 15px; box-shadow: 0 4px 20px rgba(0,0,0,0.15); }}
                        p {{ font-size: 18px; line-height: 1.5; margin: 15px 0; color: #333; }}
                        small {{ font-size: 16px; color: #666; }}
                        @media (max-width: 480px) {{
                            .container {{ padding: 30px 15px; }}
                            .success {{ font-size: 28px; }}
                            p {{ font-size: 20px; }}
                            small {{ font-size: 18px; }}
                        }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h2 class="success">✅ Success!</h2>
                        <p>Google Drive {drive_id} connected successfully!</p>
                        <p><small>This window will close automatically...</small></p>
                    </div>
                    <script>
                        setTimeout(() => {{
                            window.close();
                        }}, 2000);
                    </script>
                </body>
                </html>
                """
                return HTMLResponse(content=html_content)
            else:
                # Legacy format support
                user_email = state.replace('user:', '').split(':')[0]
                print(f"Legacy format - User: {user_email}")
                
                user_tokens = get_user_drive_tokens_from_firestore(user_email)
                token_data = json.loads(credentials.to_json())
                user_tokens['drive_1'] = token_data
                save_user_drive_tokens_to_firestore(user_email, user_tokens)
                    
                print(f"Legacy format - Successfully saved tokens for {user_email} - drive_1")
                    
                # For popup windows, return a simple HTML page that closes the popup
                html_content = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Authorization Complete</title>
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <style>
                        body {{ font-family: Arial, sans-serif; text-align: center; padding: 20px; background: #f8f9fa; margin: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center; }}
                        .success {{ color: #28a745; margin-bottom: 20px; font-size: 24px; font-weight: bold; }}
                        .container {{ max-width: 90%; width: 400px; margin: 0 auto; background: white; padding: 40px 20px; border-radius: 15px; box-shadow: 0 4px 20px rgba(0,0,0,0.15); }}
                        p {{ font-size: 18px; line-height: 1.5; margin: 15px 0; color: #333; }}
                        small {{ font-size: 16px; color: #666; }}
                        @media (max-width: 480px) {{
                            .container {{ padding: 30px 15px; }}
                            .success {{ font-size: 28px; }}
                            p {{ font-size: 20px; }}
                            small {{ font-size: 18px; }}
                        }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h2 class="success">✅ Success!</h2>
                        <p>Google Drive connected successfully!</p>
                        <p><small>This window will close automatically...</small></p>
                    </div>
                    <script>
                        setTimeout(() => {{
                            window.close();
                        }}, 2000);
                    </script>
                </body>
                </html>
                """
                return HTMLResponse(content=html_content)
        else:
            # Save shared tokens for admin setup
            save_json_file('shared_tokens.json', json.loads(credentials.to_json()))
            return {"message": "Shared Google Drive authorized successfully!"}
            
    except Exception as e:
        print(f"OAuth callback error: {str(e)}")
        
        if state and state.startswith('user:'):
            # For popup windows, return error HTML page
            error_message = "Connection expired. Please try again." if 'invalid_grant' in str(e).lower() else str(e)
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Authorization Error</title>
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <style>
                    body {{ font-family: Arial, sans-serif; text-align: center; padding: 20px; background: #f8f9fa; margin: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center; }}
                    .error {{ color: #dc3545; margin-bottom: 20px; font-size: 24px; font-weight: bold; }}
                    .container {{ max-width: 90%; width: 400px; margin: 0 auto; background: white; padding: 40px 20px; border-radius: 15px; box-shadow: 0 4px 20px rgba(0,0,0,0.15); }}
                    p {{ font-size: 18px; line-height: 1.5; margin: 15px 0; color: #333; }}
                    small {{ font-size: 16px; color: #666; }}
                    @media (max-width: 480px) {{
                        .container {{ padding: 30px 15px; }}
                        .error {{ font-size: 28px; }}
                        p {{ font-size: 20px; }}
                        small {{ font-size: 18px; }}
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h2 class="error">❌ Error</h2>
                    <p>{error_message}</p>
                    <p><small>This window will close automatically...</small></p>
                </div>
                <script>
                    setTimeout(() => {{
                        window.close();
                    }}, 3000);
                </script>
            </body>
            </html>
            """
            return HTMLResponse(content=html_content)
        return {"error": f"Authorization failed: {str(e)}"}

@app.post("/batch-create-folders")
async def batch_create_folders(
    folder_names: List[str] = Form(...),
    parent_folder_id: Optional[str] = Form(None),
    use_personal_drive: bool = Form(False),
    drive_id: str = Form("drive_1"),
    overwrite: bool = Form(False),
    current_user: str = Depends(get_current_user)
):
    
    results = []
    errors = []
    
    for folder_name in folder_names:
        try:
            result = await create_single_folder(
                folder_name=folder_name,
                parent_folder_id=parent_folder_id,
                use_personal_drive=use_personal_drive,
                drive_id=drive_id,
                overwrite=overwrite,
                current_user=current_user
            )
            results.append({"folder": folder_name, "success": True, "data": result})
        except Exception as e:
            errors.append({"folder": folder_name, "error": str(e)})
    
    return {
        "total_folders": len(folder_names),
        "successful_creations": len(results),
        "failed_creations": len(errors),
        "results": results,
        "errors": errors
    }

@app.post("/create-folder")
async def create_folder(
    folder_name: str = Form(...),
    parent_folder_id: Optional[str] = Form(None),
    use_personal_drive: bool = Form(False),
    drive_id: str = Form("drive_1"),
    overwrite: bool = Form(False),
    current_user: str = Depends(get_current_user)
):
    return await create_single_folder(
        folder_name=folder_name,
        parent_folder_id=parent_folder_id,
        use_personal_drive=use_personal_drive,
        drive_id=drive_id,
        overwrite=overwrite,
        current_user=current_user
    )

async def create_single_folder(
    folder_name: str,
    parent_folder_id: Optional[str] = None,
    use_personal_drive: bool = False,
    drive_id: str = "drive_1",
    overwrite: bool = False,
    current_user: str = None
):
    if use_personal_drive:
        service = get_user_google_service(current_user, drive_id)
        if not service:
            raise HTTPException(status_code=400, detail=f"Personal Google Drive {drive_id} not connected")
        default_parent = 'root'
    else:
        service = get_google_service()
        if not service:
            raise HTTPException(status_code=500, detail="Google Drive not configured")
        
        user_data = get_user_from_firestore(current_user)
        if not user_data:
            raise HTTPException(status_code=404, detail="User not found")
        
        user_folder_id = user_data.get('folder_id')
        if not user_folder_id:
            user_folder_id = get_user_folder(service, user_data['name'])
            update_user_in_firestore(current_user, {'folder_id': user_folder_id})
        default_parent = user_folder_id
    
    target_parent = parent_folder_id if parent_folder_id else default_parent
    
    try:
        # Check if folder already exists
        existing_folders = service.files().list(
            q=f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and '{target_parent}' in parents and trashed=false",
            fields="files(id, name)"
        ).execute().get('files', [])
        
        if existing_folders and not overwrite:
            raise HTTPException(
                status_code=409,
                detail={
                    "conflict": True,
                    "type": "folder",
                    "name": folder_name,
                    "message": f"Folder '{folder_name}' already exists. Do you want to overwrite it?",
                    "existing_id": existing_folders[0]['id']
                }
            )
        elif existing_folders and overwrite:
            # Delete existing folder to overwrite
            service.files().delete(fileId=existing_folders[0]['id']).execute()
        
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [target_parent]
        }
        
        folder = service.files().create(body=folder_metadata, fields='id,name,mimeType,createdTime').execute()
        return {"id": folder['id'], "name": folder['name'], "mimeType": folder['mimeType'], "createdTime": folder['createdTime']}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Folder creation failed: {str(e)}")



@app.post("/batch-upload")
async def batch_upload_files(
    files: List[UploadFile] = File(...),
    folder_id: Optional[str] = Form(None),
    folder_paths: Optional[List[str]] = Form(None),
    use_personal_drive: bool = Form(False),
    drive_id: str = Form("drive_1"),
    overwrite: bool = Form(False),
    current_user: str = Depends(get_current_user)
):
    
    results = []
    errors = []
    
    for i, file in enumerate(files):
        try:
            folder_path = folder_paths[i] if folder_paths and i < len(folder_paths) else None
            result = await upload_single_file(
                file=file,
                folder_id=folder_id,
                folder_path=folder_path,
                use_personal_drive=use_personal_drive,
                drive_id=drive_id,
                overwrite=overwrite,
                current_user=current_user
            )
            results.append({"file": file.filename, "success": True, "data": result})
        except Exception as e:
            errors.append({"file": file.filename, "error": str(e)})
    
    return {
        "total_files": len(files),
        "successful_uploads": len(results),
        "failed_uploads": len(errors),
        "results": results,
        "errors": errors
    }

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    folder_id: Optional[str] = Form(None),
    folder_path: Optional[str] = Form(None),
    use_personal_drive: bool = Form(False),
    drive_id: str = Form("drive_1"),
    overwrite: bool = Form(False),
    current_user: str = Depends(get_current_user)
):
    return await upload_single_file(
        file=file,
        folder_id=folder_id,
        folder_path=folder_path,
        use_personal_drive=use_personal_drive,
        drive_id=drive_id,
        overwrite=overwrite,
        current_user=current_user
    )

async def upload_single_file(
    file: UploadFile,
    folder_id: Optional[str] = None,
    folder_path: Optional[str] = None,
    use_personal_drive: bool = False,
    drive_id: str = "drive_1",
    overwrite: bool = False,
    current_user: str = None
):
    try:
        # Validate file
        if not file or not file.filename:
            raise HTTPException(status_code=400, detail="No file provided")
        
        # Choose between personal drive or shared drive
        if use_personal_drive:
            service = get_user_google_service(current_user, drive_id)
            if not service:
                raise HTTPException(status_code=400, detail=f"Personal Google Drive {drive_id} not connected. Please connect your Google Drive first.")
        else:
            service = get_google_service()
            if not service:
                raise HTTPException(status_code=500, detail="Shared Google Drive not configured. Please contact administrator.")
    except HTTPException:
        raise
    except Exception as e:
        print(f"Upload initialization error: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to initialize upload service")
    
    if use_personal_drive:
        # For personal drive, use root as default
        target_folder_id = 'root'
    else:
        user_data = get_user_from_firestore(current_user)
        if not user_data:
            raise HTTPException(status_code=404, detail="User not found")
        
        user_folder_id = user_data.get('folder_id')
        if not user_folder_id:
            user_folder_id = get_user_folder(service, current_user)
            if not user_folder_id:
                raise HTTPException(status_code=500, detail="Failed to create or access user folder")
            update_user_in_firestore(current_user, {'folder_id': user_folder_id})
        
        # Verify folder still exists
        try:
            service.files().get(fileId=user_folder_id).execute()
            target_folder_id = user_folder_id
        except HttpError as e:
            if e.resp.status == 404:
                # Folder was deleted, create new one
                print(f"User folder {user_folder_id} not found, creating new one")
                user_folder_id = get_user_folder(service, user_data['name'])
                if not user_folder_id:
                    raise HTTPException(status_code=500, detail="Failed to create user folder")
                update_user_in_firestore(current_user, {'folder_id': user_folder_id})
                target_folder_id = user_folder_id
            else:
                raise
    
    try:
        # Handle folder path for folder uploads
        if folder_path:
            # Create nested folders if needed
            folders = folder_path.strip('/').split('/')
            current_folder_id = target_folder_id
            for folder_name in folders:
                if folder_name:
                    # Check if folder exists
                    results = service.files().list(
                        q=f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and '{current_folder_id}' in parents and trashed=false",
                        fields="files(id, name)"
                    ).execute()
                    
                    existing_folders = results.get('files', [])
                    if existing_folders:
                        current_folder_id = existing_folders[0]['id']
                    else:
                        # Create folder only if it doesn't exist
                        try:
                            folder_metadata = {
                                'name': folder_name,
                                'mimeType': 'application/vnd.google-apps.folder',
                                'parents': [current_folder_id]
                            }
                            new_folder = service.files().create(body=folder_metadata, fields='id').execute()
                            current_folder_id = new_folder['id']
                        except HttpError as e:
                            if 'already exists' in str(e).lower():
                                # Folder was created by another request, find it
                                results = service.files().list(
                                    q=f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and '{current_folder_id}' in parents and trashed=false",
                                    fields="files(id, name)"
                                ).execute()
                                existing_folders = results.get('files', [])
                                if existing_folders:
                                    current_folder_id = existing_folders[0]['id']
                                else:
                                    raise
                            else:
                                raise
            target_folder_id = current_folder_id
        elif folder_id:
            target_folder_id = folder_id
        
        # Read file content
        file_content = await file.read()
        if not file_content:
            raise HTTPException(status_code=400, detail="File is empty")
        
        # Check if file exists
        existing_files = service.files().list(
            q=f"name='{file.filename}' and '{target_folder_id}' in parents and trashed=false",
            fields="files(id, name)"
        ).execute().get('files', [])
        
        if existing_files and not overwrite:
            raise HTTPException(
                status_code=409,
                detail={
                    "conflict": True,
                    "type": "file",
                    "name": file.filename,
                    "message": f"File '{file.filename}' already exists. Do you want to overwrite it?",
                    "existing_id": existing_files[0]['id']
                }
            )
        elif existing_files and overwrite:
            # Delete existing file to overwrite
            service.files().delete(fileId=existing_files[0]['id']).execute()
        
        # Determine MIME type based on file extension if not provided
        content_type = file.content_type
        if not content_type or content_type == 'application/octet-stream':
            import mimetypes
            content_type, _ = mimetypes.guess_type(file.filename)
            if not content_type:
                content_type = 'application/octet-stream'
        
        file_metadata = {
            'name': file.filename,
            'parents': [target_folder_id],
            'mimeType': content_type
        }
        
        media = MediaIoBaseUpload(
            io.BytesIO(file_content),
            mimetype=content_type,
            resumable=True
        )
        
        uploaded_file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id,name,size,mimeType,createdTime,webViewLink,webContentLink'
        ).execute()
        
        # Update user storage in real-time (only for shared drive)
        if not use_personal_drive:
            print(f"Updating storage for user: {current_user}")
            update_user_storage(current_user)
        
        return FileInfo(**uploaded_file)
        
    except HTTPException:
        raise
    except HttpError as e:
        error_details = e.error_details[0] if e.error_details else {}
        error_reason = error_details.get('reason', 'unknown')
        
        if error_reason == 'storageQuotaExceeded':
            raise HTTPException(status_code=400, detail="Storage quota exceeded. Please free up space or upgrade your storage.")
        elif error_reason == 'rateLimitExceeded':
            raise HTTPException(status_code=429, detail="Rate limit exceeded. Please try again later.")
        elif error_reason == 'authError':
            raise HTTPException(status_code=401, detail="Authentication failed. Please reconnect your Google Drive.")
        else:
            print(f"Google Drive API error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Google Drive error: {error_reason}")
    except Exception as e:
        print(f"Upload error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.post("/batch-operations")
async def batch_operations(
    operations: List[dict],
    current_user: str = Depends(get_current_user)
):
    """Process multiple operations in parallel for improved performance"""
    try:
        results = await parallel_processor.batch_file_operations(operations)
        return {
            "success": True,
            "results": results,
            "total_operations": len(operations),
            "successful_operations": sum(1 for r in results if r.get('success'))
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Batch operation failed: {str(e)}")

def get_drive_service(current_user: str, use_personal_drive: bool = False, drive_id: str = "drive_1"):
    """Unified service getter for both shared and personal drives"""
    if use_personal_drive:
        return get_user_google_service(current_user, drive_id)
    return get_google_service()

@app.post("/direct-urls")
async def get_direct_urls(
    file_ids: List[str],
    use_personal_drive: bool = False,
    drive_id: str = "drive_1",
    current_user: Optional[str] = Depends(get_optional_current_user)
):
    """Get direct Google Drive URLs for multiple files"""
    if use_personal_drive and not current_user:
        raise HTTPException(status_code=401, detail="Authentication required for personal drive")
    
    service = get_drive_service(current_user or '', use_personal_drive, drive_id)
    if not service:
        raise HTTPException(status_code=400, detail="Google Drive service not available")
    
    from parallel_api import get_direct_download_urls
    results = await get_direct_download_urls(file_ids, service)
    
    return {
        "success": True,
        "direct_urls": results,
        "total_files": len(file_ids)
    }

@app.get("/stream/{file_id}")
async def stream_file(
    file_id: str,
    use_personal_drive: bool = False,
    drive_id: str = "drive_1",
    current_user: Optional[str] = Depends(get_optional_current_user)
):
    """Stream file directly from Google Drive without downloading to server"""
    if use_personal_drive and not current_user:
        raise HTTPException(status_code=401, detail="Authentication required for personal drive")
    
    service = get_drive_service(current_user or '', use_personal_drive, drive_id)
    if not service:
        raise HTTPException(status_code=400, detail="Google Drive service not available")
    
    file_metadata = service.files().get(fileId=file_id, fields='id,name,mimeType,size').execute()
    
    def generate_stream():
        request = service.files().get_media(fileId=file_id)
        file_io = io.BytesIO()
        file_size = int(file_metadata.get('size', 0))
        chunk_size = get_optimal_chunk_size(file_size)
        downloader = MediaIoBaseDownload(file_io, request, chunksize=chunk_size)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                file_io.seek(0)
                chunk = file_io.read()
                if chunk:
                    yield chunk
                file_io.seek(0)
                file_io.truncate(0)
    
    # Sanitize filename for headers
    import re
    safe_filename = re.sub(r'[<>:"/\\|?*]', '_', file_metadata["name"])
    safe_filename = safe_filename.encode('ascii', 'ignore').decode('ascii')
    if not safe_filename or safe_filename.strip() == '':
        safe_filename = 'file'
    
    return StreamingResponse(
        generate_stream(),
        media_type=file_metadata.get('mimeType', 'application/octet-stream'),
        headers={
            'Content-Disposition': f'inline; filename="{safe_filename}"',
            'Cache-Control': 'public, max-age=3600',
            'Accept-Ranges': 'bytes'
        }
    )

@app.post("/batch-stream-setup")
async def setup_batch_streaming(
    file_ids: List[str],
    use_personal_drive: bool = False,
    drive_id: str = "drive_1",
    current_user: Optional[str] = Depends(get_optional_current_user)
):
    """Setup streaming for multiple files"""
    if use_personal_drive and not current_user:
        raise HTTPException(status_code=401, detail="Authentication required for personal drive")
    
    service = get_drive_service(current_user or '', use_personal_drive, drive_id)
    if not service:
        raise HTTPException(status_code=400, detail="Google Drive service not available")
    
    from parallel_api import setup_streaming_downloads
    results = await setup_streaming_downloads(file_ids, service)
    
    return {
        "success": True,
        "stream_setups": results,
        "total_files": len(file_ids)
    }

@app.get("/parallel-data")
async def get_parallel_data(
    use_personal_drive: bool = False,
    drive_id: str = "drive_1",
    current_user: str = Depends(get_current_user)
):
    """Get files, storage, and quota data in parallel"""
    async def get_files_async():
        return await list_files(use_personal_drive, drive_id, current_user)
    
    async def get_storage_async():
        try:
            return await get_user_storage(current_user)
        except:
            return None
    
    async def get_quota_async():
        try:
            return await get_drive_quota_info(use_personal_drive, drive_id, current_user)
        except:
            return None
    
    # Run all operations in parallel
    files_task = asyncio.create_task(get_files_async())
    storage_task = asyncio.create_task(get_storage_async())
    quota_task = asyncio.create_task(get_quota_async())
    
    files, storage, quota = await asyncio.gather(
        files_task, storage_task, quota_task, return_exceptions=True
    )
    
    return {
        "files": files if not isinstance(files, Exception) else [],
        "storage": storage if not isinstance(storage, Exception) else None,
        "quota": quota if not isinstance(quota, Exception) else None
    }

@app.get("/files", response_model=List[FileInfo])
async def list_files(
    use_personal_drive: bool = False,
    drive_id: str = "drive_1",
    show_all: bool = False,
    current_user: str = Depends(get_current_user)
):
    if use_personal_drive:
        service = get_user_google_service(current_user, drive_id)
        if not service:
            return []
        if show_all:
            folder_query = "trashed=false"
            print(f"Personal drive show_all query: {folder_query}")
        else:
            folder_query = "'root' in parents and trashed=false"
    else:
        service = get_google_service()
        if not service:
            return []
        
        user_data = get_user_from_firestore(current_user)
        if not user_data:
            # User not found in Firestore, create minimal user record
            try:
                firebase_user = auth.get_user_by_email(current_user)
                new_user = {
                    "email": current_user,
                    "name": firebase_user.display_name or "User",
                    "uid": firebase_user.uid,
                    "created_at": datetime.utcnow().isoformat(),
                    "storage_used": 0,
                    "total_files": 0,
                    "total_folders": 0,
                    "last_login": datetime.utcnow().isoformat(),
                    "folder_id": None
                }
                save_user_to_firestore(current_user, new_user)
                user_data = new_user
            except Exception as e:
                print(f"Error creating user record: {str(e)}")
                return []
        
        user_folder_id = user_data.get('folder_id')
        if not user_folder_id:
            # Create user folder if it doesn't exist
            user_folder_id = get_user_folder(service, current_user)
            if user_folder_id:
                update_user_in_firestore(current_user, {'folder_id': user_folder_id})
            else:
                return []
        
        if show_all:
            # For shared drive, show all files within user's folder recursively
            folder_query = f"'{user_folder_id}' in parents and trashed=false"
        else:
            folder_query = f"'{user_folder_id}' in parents and trashed=false"
    
    try:
        all_files = []
        page_token = None
        
        while True:
            params = {
                'q': folder_query,
                'fields': "nextPageToken,files(id,name,size,mimeType,createdTime,webViewLink,webContentLink,parents)",
                'pageSize': 1000
            }
            if page_token:
                params['pageToken'] = page_token
                
            results = service.files().list(**params).execute()
            files = results.get('files', [])
            all_files.extend(files)
            
            page_token = results.get('nextPageToken')
            if not page_token:
                break
        
        # Build folder path cache for better performance
        folder_cache = {}
        if show_all and use_personal_drive:
            try:
                # Get all folders first to build path cache
                folder_results = service.files().list(
                    q="mimeType='application/vnd.google-apps.folder' and trashed=false",
                    fields="files(id,name,parents)",
                    pageSize=1000
                ).execute()
                
                for folder in folder_results.get('files', []):
                    folder_cache[folder['id']] = folder.get('name', '')
            except:
                pass
        
        # Process files and add folder paths for show_all mode
        processed_files = []
        for file in all_files:
            if show_all and use_personal_drive and file.get('parents'):
                # Get folder path for better organization
                try:
                    parent_id = file['parents'][0]
                    if parent_id != 'root' and parent_id in folder_cache:
                        file['folder_path'] = folder_cache[parent_id]
                    elif parent_id != 'root':
                        try:
                            parent = service.files().get(fileId=parent_id, fields='name').execute()
                            file['folder_path'] = parent.get('name', '')
                            folder_cache[parent_id] = file['folder_path']
                        except:
                            file['folder_path'] = 'Unknown Folder'
                    else:
                        file['folder_path'] = 'Root'
                except:
                    file['folder_path'] = ''
            
            if file.get('mimeType') == 'application/vnd.google-apps.folder':
                # Calculate folder size only for non-show-all mode to improve performance
                if not show_all:
                    try:
                        folder_files = service.files().list(
                            q=f"'{file['id']}' in parents and trashed=false",
                            fields="files(size)"
                        ).execute().get('files', [])
                        total_size = sum(int(f.get('size', 0)) for f in folder_files if f.get('size'))
                        file['size'] = total_size
                    except:
                        file['size'] = 0
                else:
                    file['size'] = 0
            
            processed_files.append(FileInfo(**file))
        
        return processed_files
    except Exception as e:
        print(f"Error listing files for user {current_user}, personal_drive={use_personal_drive}, show_all={show_all}: {str(e)}")
        return []

@app.get("/folder/{folder_id}/contents", response_model=List[FileInfo])
async def get_folder_contents(
    folder_id: str, 
    recursive: bool = False,
    use_personal_drive: bool = False,
    drive_id: str = "drive_1",
    current_user: str = Depends(get_current_user)
):
    if use_personal_drive:
        service = get_user_google_service(current_user, drive_id)
    else:
        service = get_google_service()
    
    if not service:
        return []
    
    try:
        def get_folder_contents_recursive(folder_id, path=""):
            results = service.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                fields="files(id,name,size,mimeType,createdTime,webViewLink,webContentLink)"
            ).execute()
            files = results.get('files', [])
            
            all_files = []
            for file in files:
                file_info = file.copy()
                if path:
                    file_info['name'] = f"{path}/{file['name']}"
                    file_info['path'] = path
                    file_info['basename'] = file['name']
                else:
                    file_info['path'] = ""
                    file_info['basename'] = file['name']
                
                all_files.append(file_info)
                
                # If recursive and it's a folder, get its contents
                if recursive and file.get('mimeType') == 'application/vnd.google-apps.folder':
                    subfolder_files = get_folder_contents_recursive(file['id'], file_info['name'])
                    all_files.extend(subfolder_files)
            
            return all_files
        
        files = get_folder_contents_recursive(folder_id)
        return [FileInfo(**file) for file in files]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch folder contents: {str(e)}")

@app.get("/preview/{file_id}")
async def preview_file(
    file_id: str, 
    use_personal_drive: bool = False,
    drive_id: str = "drive_1",
    current_user: Optional[str] = Depends(get_optional_current_user)
):
    try:
        if use_personal_drive and not current_user:
            raise HTTPException(status_code=401, detail="Authentication required for personal drive access")
        
        service = get_drive_service(current_user or '', use_personal_drive, drive_id)
        if not service:
            drive_type = "Personal" if use_personal_drive else "Shared"
            raise HTTPException(status_code=400, detail=f"{drive_type} Google Drive not available")
        
        # Get file metadata first
        file_metadata = service.files().get(fileId=file_id, fields='id,name,mimeType,size').execute()
        
        # Stream file content directly without downloading to server
        def generate_stream():
            request = service.files().get_media(fileId=file_id)
            file_io = io.BytesIO()
            downloader = MediaIoBaseDownload(file_io, request, chunksize=1024*1024)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    file_io.seek(0)
                    chunk = file_io.read()
                    if chunk:
                        yield chunk
                    file_io.seek(0)
                    file_io.truncate(0)
        
        # Determine MIME type and headers
        mime_type = file_metadata.get('mimeType', 'application/octet-stream')
        file_name = file_metadata.get('name', '').lower()
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'public, max-age=3600',
            'Content-Disposition': 'inline'  # Default to inline for preview
        }
        
        # Enhanced file type handling for all extensions
        file_ext = file_name.split('.')[-1] if '.' in file_name else ''
        
        # Text files
        if file_ext in ['txt', 'csv', 'log', 'md', 'py', 'js', 'jsx', 'ts', 'tsx', 'css', 'html', 'htm', 'json', 'xml', 'yaml', 'yml', 'sql', 'php', 'rb', 'go', 'rs', 'kt', 'swift', 'dart', 'sh', 'bat', 'ps1', 'c', 'cpp', 'h', 'hpp', 'java', 'scala', 'r', 'lua', 'm', 'pl', 'ini', 'cfg', 'conf', 'toml']:
            mime_type = 'text/plain; charset=utf-8'
        # Images
        elif file_ext in ['jpg', 'jpeg']:
            mime_type = 'image/jpeg'
        elif file_ext == 'png':
            mime_type = 'image/png'
        elif file_ext == 'gif':
            mime_type = 'image/gif'
        elif file_ext == 'svg':
            mime_type = 'image/svg+xml'
        elif file_ext in ['bmp', 'webp', 'ico', 'tiff', 'tif']:
            mime_type = f'image/{file_ext}'
        # Documents
        elif file_ext == 'pdf':
            mime_type = 'application/pdf'
        elif file_ext in ['doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx']:
            mime_type = 'application/octet-stream'  # Force download for Office files
        # Videos
        elif file_ext in ['mp4', 'avi', 'mov', 'wmv', 'flv', 'webm', 'mkv', 'm4v', '3gp', 'ogv']:
            mime_type = f'video/{"mp4" if file_ext == "m4v" else file_ext}'
            headers['Accept-Ranges'] = 'bytes'
        # Audio
        elif file_ext in ['mp3', 'wav', 'ogg', 'aac', 'flac', 'm4a', 'wma']:
            mime_type = f'audio/{"mpeg" if file_ext == "mp3" else file_ext}'
            headers['Accept-Ranges'] = 'bytes'
        # Archives
        elif file_ext in ['zip', 'rar', '7z', 'tar', 'gz', 'bz2', 'xz']:
            mime_type = 'application/octet-stream'
        
        return StreamingResponse(
            generate_stream(),
            media_type=mime_type,
            headers=headers
        )
        
    except HttpError as e:
        if e.resp.status == 404:
            raise HTTPException(status_code=404, detail="File not found or has been deleted")
        elif e.resp.status == 403:
            raise HTTPException(status_code=403, detail="Access denied to file. Check permissions.")
        elif e.resp.status == 429:
            raise HTTPException(status_code=429, detail="Rate limit exceeded. Please try again later.")
        else:
            print(f"Google Drive API error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Google Drive error: {e.resp.status}")
    except Exception as e:
        print(f"Preview error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Preview failed: {str(e)}")

@app.get("/download/{file_id}")
async def download_file(
    file_id: str, 
    use_personal_drive: bool = False,
    drive_id: str = "drive_1",
    current_user: Optional[str] = Depends(get_optional_current_user)
):
    try:
        # For personal drive, authentication is required
        if use_personal_drive and not current_user:
            raise HTTPException(status_code=401, detail="Authentication required for personal drive access")
        
        service = get_drive_service(current_user or '', use_personal_drive, drive_id)
        if not service:
            drive_type = "Personal" if use_personal_drive else "Shared"
            raise HTTPException(status_code=400, detail=f"{drive_type} Google Drive not available")
        
        file_metadata = service.files().get(fileId=file_id, fields='id,name,mimeType,size').execute()
        
        # Check if it's a folder
        if file_metadata.get('mimeType') == 'application/vnd.google-apps.folder':
            # For folders, create a ZIP file
            import zipfile
            import tempfile
            
            def generate_folder_zip():
                with tempfile.NamedTemporaryFile() as temp_file:
                    with zipfile.ZipFile(temp_file, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                        # Get all files in the folder
                        results = service.files().list(
                            q=f"'{file_id}' in parents and trashed=false",
                            fields="files(id,name,mimeType)"
                        ).execute()
                        
                        for file_item in results.get('files', []):
                            if file_item.get('mimeType') != 'application/vnd.google-apps.folder':
                                try:
                                    # Download file content
                                    request = service.files().get_media(fileId=file_item['id'])
                                    file_io = io.BytesIO()
                                    downloader = MediaIoBaseDownload(file_io, request)
                                    done = False
                                    while not done:
                                        status, done = downloader.next_chunk()
                                    
                                    # Add to ZIP
                                    zip_file.writestr(file_item['name'], file_io.getvalue())
                                except Exception as e:
                                    print(f"Error adding file {file_item['name']} to ZIP: {str(e)}")
                    
                    temp_file.seek(0)
                    while True:
                        chunk = temp_file.read(8192)
                        if not chunk:
                            break
                        yield chunk
            
            folder_name = file_metadata['name']
            safe_folder_name = re.sub(r'[<>:"/\\|?*]', '_', folder_name)
            # Remove Unicode characters that can't be encoded in Latin-1
            safe_folder_name = safe_folder_name.encode('ascii', 'ignore').decode('ascii')
            if not safe_folder_name or safe_folder_name.strip() == '':
                safe_folder_name = 'folder'
            
            return StreamingResponse(
                generate_folder_zip(),
                media_type='application/zip',
                headers={
                    "Content-Disposition": f'attachment; filename="{safe_folder_name}.zip"',
                    "Cache-Control": "no-cache"
                }
            )
        
        # For regular files
        def generate_stream():
            request = service.files().get_media(fileId=file_id)
            file_io = io.BytesIO()
            file_size = int(file_metadata.get('size', 0))
            chunk_size = get_optimal_chunk_size(file_size)
            downloader = MediaIoBaseDownload(file_io, request, chunksize=chunk_size)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    file_io.seek(0)
                    chunk = file_io.read()
                    if chunk:
                        yield chunk
                    file_io.seek(0)
                    file_io.truncate(0)
        
        filename = file_metadata['name']
        file_extension = filename.lower().split('.')[-1] if '.' in filename else ''
        
        # Sanitize filename for download while preserving extension
        import re
        safe_filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Remove Unicode characters that can't be encoded in Latin-1
        safe_filename = safe_filename.encode('ascii', 'ignore').decode('ascii')
        if not safe_filename or safe_filename.strip() == '':
            safe_filename = f'file.{file_extension}' if file_extension else 'file'
        
        # Comprehensive MIME type mapping
        mime_types = {
            # Images
            'png': 'image/png', 'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'gif': 'image/gif',
            'bmp': 'image/bmp', 'webp': 'image/webp', 'svg': 'image/svg+xml', 'ico': 'image/x-icon',
            'tiff': 'image/tiff', 'tif': 'image/tiff',
            # Documents
            'pdf': 'application/pdf', 'txt': 'text/plain', 'csv': 'text/csv', 'md': 'text/markdown',
            'json': 'application/json', 'xml': 'application/xml', 'html': 'text/html', 'htm': 'text/html',
            'css': 'text/css', 'js': 'application/javascript', 'jsx': 'text/jsx', 'ts': 'text/typescript',
            'tsx': 'text/tsx', 'py': 'text/x-python', 'java': 'text/x-java-source', 'c': 'text/x-c',
            'cpp': 'text/x-c++', 'h': 'text/x-c', 'hpp': 'text/x-c++', 'php': 'text/x-php',
            'rb': 'text/x-ruby', 'go': 'text/x-go', 'rs': 'text/x-rust', 'kt': 'text/x-kotlin',
            'swift': 'text/x-swift', 'dart': 'text/x-dart', 'sh': 'text/x-shellscript',
            'bat': 'text/x-msdos-batch', 'ps1': 'text/x-powershell', 'sql': 'text/x-sql',
            'yaml': 'text/yaml', 'yml': 'text/yaml', 'toml': 'text/x-toml', 'ini': 'text/plain',
            'cfg': 'text/plain', 'conf': 'text/plain', 'log': 'text/plain',
            # Office documents
            'doc': 'application/msword', 'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'xls': 'application/vnd.ms-excel', 'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'ppt': 'application/vnd.ms-powerpoint', 'pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            # Archives
            'zip': 'application/zip', 'rar': 'application/x-rar-compressed', '7z': 'application/x-7z-compressed',
            'tar': 'application/x-tar', 'gz': 'application/gzip', 'bz2': 'application/x-bzip2',
            'xz': 'application/x-xz',
            # Videos
            'mp4': 'video/mp4', 'avi': 'video/x-msvideo', 'mov': 'video/quicktime', 'wmv': 'video/x-ms-wmv',
            'flv': 'video/x-flv', 'webm': 'video/webm', 'mkv': 'video/x-matroska', 'm4v': 'video/mp4',
            '3gp': 'video/3gpp', 'ogv': 'video/ogg',
            # Audio
            'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'ogg': 'audio/ogg', 'aac': 'audio/aac',
            'flac': 'audio/flac', 'm4a': 'audio/mp4', 'wma': 'audio/x-ms-wma',
            # Executables
            'exe': 'application/x-msdownload', 'msi': 'application/x-msi', 'dmg': 'application/x-apple-diskimage',
            'deb': 'application/x-debian-package', 'rpm': 'application/x-rpm', 'apk': 'application/vnd.android.package-archive',
            'iso': 'application/x-iso9660-image'
        }
        proper_mime_type = mime_types.get(file_extension, 'application/octet-stream')
        
        file_size = int(file_metadata.get('size', 0))
        return StreamingResponse(
            generate_stream(),
            media_type=proper_mime_type,
            headers={
                "Content-Disposition": f'attachment; filename="{safe_filename}"',
                "Cache-Control": "public, max-age=3600",
                "Accept-Ranges": "bytes",
                "Connection": "keep-alive",
                "Content-Length": str(file_size)
            }
        )
        
    except HttpError as e:
        if e.resp.status == 404:
            raise HTTPException(status_code=404, detail="File not found or has been deleted")
        elif e.resp.status == 403:
            raise HTTPException(status_code=403, detail="Access denied to file")
        else:
            print(f"Download error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Download failed: {e.resp.status}")
    except Exception as e:
        print(f"Download error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")

@app.delete("/delete/{file_id}")
async def delete_file(
    file_id: str, 
    use_personal_drive: bool = False,
    drive_id: str = "drive_1",
    current_user: str = Depends(get_current_user)
):
    if use_personal_drive:
        service = get_user_google_service(current_user, drive_id)
    else:
        service = get_google_service()
    
    if not service:
        raise HTTPException(status_code=500, detail="Google Drive not configured")
    
    try:
        service.files().delete(fileId=file_id).execute()
        # Update user storage in real-time (only for shared drive)
        if not use_personal_drive:
            update_user_storage(current_user)
        return {"message": "File deleted successfully"}
    except Exception as e:
        if "File not found" in str(e) or "404" in str(e):
            return {"message": "File already deleted or not found"}
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")

@app.post("/batch-delete")
async def batch_delete_files(
    file_ids: List[str],
    use_personal_drive: bool = False,
    drive_id: str = "drive_1",
    current_user: str = Depends(get_current_user)
):
    """Delete multiple files in batch"""
    if use_personal_drive:
        service = get_user_google_service(current_user, drive_id)
    else:
        service = get_google_service()
    
    if not service:
        raise HTTPException(status_code=500, detail="Google Drive not configured")
    
    results = []
    for file_id in file_ids:
        try:
            service.files().delete(fileId=file_id).execute()
            results.append({"file_id": file_id, "success": True})
        except Exception as e:
            results.append({"file_id": file_id, "success": False, "error": str(e)})
    
    # Update user storage in real-time (only for shared drive)
    if not use_personal_drive:
        update_user_storage(current_user)
    
    successful_deletes = sum(1 for r in results if r["success"])
    return {
        "message": f"Deleted {successful_deletes} of {len(file_ids)} files",
        "results": results,
        "total_files": len(file_ids),
        "successful_deletes": successful_deletes
    }













def update_platform_stats():
    """Update platform statistics"""
    pass  # No longer needed

@app.get("/stats")
async def get_statistics():
    """Get real-time platform statistics"""
    try:
        current_time = datetime.utcnow()
        
        # Get all users from Firestore
        all_users = get_all_users_from_firestore()
        total_registered = len(all_users)
        active_users = 0
        online_users = 0
        
        for user_data in all_users:
            if user_data.get('last_login'):
                try:
                    last_login = datetime.fromisoformat(user_data['last_login'])
                    hours_since_login = (current_time - last_login).total_seconds() / 3600
                    
                    if hours_since_login <= 24:  # Active in last 24 hours
                        active_users += 1
                    if hours_since_login <= 1:   # Online in last hour
                        online_users += 1
                except:
                    pass
        
        # Real countries from sessions
        unique_countries = set()
        for session in user_sessions.values():
            if session.get('country'):
                unique_countries.add(session['country'])
        countries_count = len(unique_countries) if unique_countries else 1
        
        # Calculate uptime
        uptime = 99.9
        
        # Real storage calculation: 5GB per registered user
        total_storage = total_registered * 5
        
        return {
            "active_users": f"{active_users}" if active_users > 0 else "0",
            "uptime": f"{uptime}%",
            "free_storage": "5GB",
            "countries": f"{countries_count}",
            "total_accounts": total_registered,
            "online_now": online_users,
            "registered_today": sum(1 for user in all_users 
                                  if user.get('created_at') and 
                                  (current_time - datetime.fromisoformat(user['created_at'])).days == 0)
        }
    except Exception as e:
        return {
            "active_users": "0",
            "uptime": "99.9%",
            "free_storage": "5GB",
            "countries": "1",
            "total_accounts": 0,
            "online_now": 0,
            "registered_today": 0
        }



def get_user_storage_data(user_email: str):
    """Get formatted storage data for a user"""
    user_data = get_user_from_firestore(user_email)
    if not user_data:
        return {
            "storage_used": 0,
            "storage_display": "0 B",
            "storage_limit": 16106127360,
            "limit_display": "15 GB",
            "storage_percentage": 0
        }
    
    storage_used = user_data.get('storage_used', 0)
    
    def format_size(size_bytes):
        if size_bytes == 0:
            return "0 B"
        size_names = ["B", "KB", "MB", "GB", "TB"]
        import math
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        if s == int(s):
            s = int(s)
        return f"{s} {size_names[i]}"
    
    storage_limit = 5368709120  # 5GB
    storage_percentage = (storage_used / storage_limit * 100) if storage_used > 0 else 0
    
    return {
        "storage_used": storage_used,
        "storage_display": format_size(storage_used),
        "storage_limit": storage_limit,
        "limit_display": "5 GB",
        "storage_percentage": min(storage_percentage, 100)
    }

def get_drive_quota():
    """Get Google Drive quota information for shared drive"""
    service = get_google_service()
    if not service:
        return None
    
    try:
        about = service.about().get(fields="storageQuota").execute()
        quota = about.get('storageQuota', {})
        return {
            'limit': int(quota.get('limit', 0)),
            'usage': int(quota.get('usage', 0)),
            'usageInDrive': int(quota.get('usageInDrive', 0))
        }
    except:
        return None



@app.websocket("/ws/{user_email}")
async def websocket_endpoint(websocket: WebSocket, user_email: str):
    await websocket.accept()
    active_connections[user_email] = websocket
    
    try:
        # Send initial storage data
        storage_data = get_user_storage_data(user_email)
        if storage_data:
            await websocket.send_json({
                'type': 'storage_update',
                'data': storage_data
            })
        
        # Keep connection alive and handle messages
        while True:
            try:
                data = await websocket.receive_text()
                # Handle any incoming messages if needed
            except WebSocketDisconnect:
                break
    except Exception as e:
        print(f"WebSocket error for {user_email}: {str(e)}")
    finally:
        active_connections.pop(user_email, None)

@app.get("/user/storage")
async def get_user_storage(current_user: str = Depends(get_current_user)):
    """Get real-time storage usage for the current user"""
    user_data = get_user_from_firestore(current_user)
    if not user_data:
        # Create user record if missing
        try:
            firebase_user = auth.get_user_by_email(current_user)
            new_user = {
                "email": current_user,
                "name": firebase_user.display_name or "User",
                "uid": firebase_user.uid,
                "created_at": datetime.utcnow().isoformat(),
                "storage_used": 0,
                "total_files": 0,
                "total_folders": 0,
                "last_login": datetime.utcnow().isoformat(),
                "folder_id": None
            }
            save_user_to_firestore(current_user, new_user)
            user_data = new_user
        except Exception as e:
            print(f"Error creating user record: {str(e)}")
            raise HTTPException(status_code=404, detail="User not found")
    
    # user_data already retrieved above
    service = get_google_service()
    
    if not service:
        return get_user_storage_data(current_user)
    
    # Create folder if doesn't exist
    user_folder_id = user_data.get('folder_id')
    if not user_folder_id:
        user_folder_id = get_user_folder(service, user_data['name'])
        if user_folder_id:
            update_user_in_firestore(current_user, {'folder_id': user_folder_id})
    
    # Calculate storage from shared drive
    total_size = 0
    if user_folder_id:
        try:
            results = service.files().list(
                q=f"'{user_folder_id}' in parents and trashed=false",
                fields="files(size,mimeType)"
            ).execute()
            
            for file in results.get('files', []):
                if file.get('mimeType') != 'application/vnd.google-apps.folder':
                    total_size += int(file.get('size', 0))
        except Exception as e:
            print(f"Error calculating storage: {str(e)}")
    
    # Update storage in Firestore
    update_user_in_firestore(current_user, {'storage_used': total_size})
    
    # Return formatted data
    def format_size(size_bytes):
        if size_bytes == 0:
            return "0 B"
        size_names = ["B", "KB", "MB", "GB", "TB"]
        import math
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        if s == int(s):
            s = int(s)
        return f"{s} {size_names[i]}"
    
    storage_limit = 5368709120  # 5GB
    
    return {
        "storage_used": total_size,
        "storage_display": format_size(total_size),
        "storage_limit": storage_limit,
        "limit_display": "5 GB",
        "storage_percentage": (total_size / storage_limit * 100) if total_size > 0 else 0
    }

@app.get("/whoami")
async def whoami(current_user: str = Depends(get_current_user)):
    """Debug endpoint to check current user"""
    user_data = get_user_from_firestore(current_user) or {}
    return {
        "email": current_user,
        "name": user_data.get('name', 'Unknown'),
        "total_files": user_data.get('total_files', 0),
        "storage_used": user_data.get('storage_used', 0),
        "folder_id": user_data.get('folder_id')
    }

@app.post("/user/refresh-storage")
async def refresh_user_storage(current_user: str = Depends(get_current_user)):
    """Force refresh storage data from Google Drive"""
    if not check_user_exists_in_firestore(current_user):
        raise HTTPException(status_code=404, detail="User not found")
    
    # Force update storage from Google Drive
    update_user_storage(current_user)
    
    # Get updated storage data
    storage_data = get_user_storage_data(current_user)
    if not storage_data:
        raise HTTPException(status_code=500, detail="Failed to get storage data")
    
    return {
        "message": "Storage data refreshed successfully",
        "data": storage_data
    }

@app.get("/drive/quota")
async def get_drive_quota_info(
    use_personal_drive: bool = False,
    drive_id: str = "drive_1",
    current_user: str = Depends(get_current_user)
):
    """Get Google Drive quota information"""
    if use_personal_drive:
        service = get_user_google_service(current_user, drive_id)
        if not service:
            raise HTTPException(status_code=400, detail=f"Personal Google Drive {drive_id} not connected")
        
        try:
            about = service.about().get(fields="storageQuota").execute()
            quota = about.get('storageQuota', {})
            return {
                "total_storage": int(quota.get('limit', 0)),
                "used_storage": int(quota.get('usage', 0)),
                "drive_usage": int(quota.get('usageInDrive', 0)),
                "available_storage": int(quota.get('limit', 0)) - int(quota.get('usage', 0)),
                "is_personal_drive": True
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to get personal Drive quota: {str(e)}")
    else:
        # Return individual user storage from Firestore instead of total shared drive
        user_data = get_user_from_firestore(current_user)
        if not user_data:
            raise HTTPException(status_code=404, detail="User not found")
        
        storage_used = user_data.get('storage_used', 0)
        storage_limit = 5368709120  # 5GB per user
        
        return {
            "total_storage": storage_limit,
            "used_storage": storage_used,
            "drive_usage": storage_used,
            "available_storage": storage_limit - storage_used,
            "is_personal_drive": False,
            "user_email": current_user
        }

@app.get("/user/profile", response_model=UserProfile)
async def get_profile(current_user: str = Depends(get_current_user)):
    user_data = get_user_from_firestore(current_user)
    if not user_data:
        raise HTTPException(status_code=404, detail="User not found")
    
    update_user_storage(current_user)
    
    user_data = user_data.copy()
    user_data.pop("hashed_password", None)
    user_data["google_connected"] = True
    user_tokens = get_user_drive_tokens_from_firestore(current_user)
    user_data["personal_drive_connected"] = any(
        get_user_google_service(current_user, drive_id) is not None 
        for drive_id in user_tokens
    )
    
    return UserProfile(**user_data)

@app.put("/user/profile")
async def update_profile(
    name: str = Form(...),
    current_user: str = Depends(get_current_user)
):
    """Update user profile name"""
    if not name or not name.strip():
        raise HTTPException(status_code=400, detail="Name cannot be empty")
    
    name = name.strip()
    
    # Get current user data
    user_data = get_user_from_firestore(current_user)
    if not user_data:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Note: Google Drive folders are now based on email addresses, not names
    # so we don't need to rename folders when users change their display names
    
    # Update in Firestore
    if not update_user_in_firestore(current_user, {'name': name}):
        raise HTTPException(status_code=500, detail="Failed to update profile")
    
    # Update Firebase Auth display name
    try:
        firebase_user = auth.get_user_by_email(current_user)
        auth.update_user(firebase_user.uid, display_name=name)
    except Exception as e:
        print(f"Failed to update Firebase display name: {str(e)}")
    
    return {"message": "Profile updated successfully", "name": name}

@app.put("/user/phone")
async def update_phone(
    phone: str = Form(...),
    current_user: str = Depends(get_current_user)
):
    """Update user phone number"""
    phone = phone.strip() if phone else ""
    
    if not update_user_in_firestore(current_user, {'phone': phone}):
        raise HTTPException(status_code=500, detail="Failed to update phone")
    
    return {"message": "Phone updated successfully", "phone": phone}

@app.post("/user/email/change-request")
async def request_email_change(
    new_email: str = Form(...),
    current_user: str = Depends(get_current_user)
):
    """Request email address change with security notification"""
    if not new_email or not new_email.strip():
        raise HTTPException(status_code=400, detail="New email cannot be empty")
    
    new_email = new_email.strip().lower()
    
    # Validate email format
    import re
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(email_pattern, new_email):
        raise HTTPException(status_code=400, detail="Invalid email format")
    
    if new_email == current_user:
        raise HTTPException(status_code=400, detail="New email is the same as current email")
    
    # Check if new email already exists
    if check_user_exists_in_firestore(new_email):
        raise HTTPException(status_code=400, detail="Email already in use by another account")
    
    # Generate verification token
    import secrets
    verification_token = secrets.token_urlsafe(32)
    
    # Store email change request
    if db:
        try:
            db.collection('email_change_requests').document(current_user).set({
                'new_email': new_email,
                'verification_token': verification_token,
                'created_at': datetime.utcnow().isoformat(),
                'expires_at': (datetime.utcnow() + timedelta(hours=24)).isoformat()
            })
        except Exception as e:
            print(f"Failed to store email change request: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to process request")
    
    # Send security notification to current email
    try:
        print(f"\n=== EMAIL CHANGE SECURITY NOTIFICATION ===")
        print(f"To: {current_user}")
        print(f"Subject: Email Address Change Request")
        print(f"""Dear User,

We received a request to change your email address from {current_user} to {new_email}.

If you made this request, please verify your new email address by clicking the link below:
{os.getenv('FRONTEND_URL', 'https://novacloud22.web.app')}/verify-email-change?token={verification_token}

If you did not request this change, please secure your account immediately by:
1. Changing your password
2. Enabling two-factor authentication
3. Contacting support if needed

This link will expire in 24 hours.

Best regards,
Novacloud Security Team""")
        print(f"==========================================\n")
    except Exception as e:
        print(f"Error sending notification: {str(e)}")
    
    return {"message": "Email change request sent. Check your current email for security notification and verification link."}

@app.post("/user/email/verify-change")
async def verify_email_change(
    token: str = Form(...)
):
    """Verify email change token and update email"""
    if not db:
        raise HTTPException(status_code=500, detail="Database not available")
    
    try:
        # Find the email change request by token
        requests_ref = db.collection('email_change_requests')
        query = requests_ref.where('verification_token', '==', token).limit(1)
        docs = list(query.stream())
        
        if not docs:
            raise HTTPException(status_code=400, detail="Invalid or expired verification token")
        
        doc = docs[0]
        request_data = doc.to_dict()
        current_user = doc.id
        
        # Check expiry
        expires_at = datetime.fromisoformat(request_data['expires_at'])
        if datetime.utcnow() > expires_at:
            doc.reference.delete()
            raise HTTPException(status_code=400, detail="Verification token expired")
        
        new_email = request_data['new_email']
        
        # Check if new email is still available
        if check_user_exists_in_firestore(new_email):
            doc.reference.delete()
            raise HTTPException(status_code=400, detail="Email already in use by another account")
        
        # Update Firebase Auth email
        try:
            firebase_user = auth.get_user_by_email(current_user)
            auth.update_user(firebase_user.uid, email=new_email)
        except Exception as e:
            raise HTTPException(status_code=500, detail="Failed to update Firebase email")
        
        # Update Firestore - move user data to new email key
        old_user_data = get_user_from_firestore(current_user)
        if old_user_data:
            old_user_data['email'] = new_email
            old_user_data['email_changed_at'] = datetime.utcnow().isoformat()
            save_user_to_firestore(new_email, old_user_data)
            
            # Delete old user data
            db.collection('users').document(current_user).delete()
            
            # Move other collections
            collections_to_move = ['user_2fa', 'user_drive_tokens']
            for collection_name in collections_to_move:
                try:
                    old_doc = db.collection(collection_name).document(current_user).get()
                    if old_doc.exists:
                        db.collection(collection_name).document(new_email).set(old_doc.to_dict())
                        db.collection(collection_name).document(current_user).delete()
                except Exception as e:
                    print(f"Failed to move {collection_name}: {str(e)}")
        
        # Send confirmation to both emails
        try:
            print(f"\n=== EMAIL CHANGE CONFIRMATION ===")
            print(f"To: {current_user} (old email)")
            print(f"Subject: Email Address Changed")
            print(f"""Your email address has been successfully changed from {current_user} to {new_email}.

If you did not make this change, please contact support immediately.""")
            print(f"\nTo: {new_email} (new email)")
            print(f"Subject: Welcome to your updated Novacloud account")
            print(f"""Your email address has been successfully updated. You can now sign in with {new_email}.""")
            print(f"=====================================\n")
        except Exception as e:
            print(f"Error sending confirmation: {str(e)}")
        
        # Clean up request
        doc.reference.delete()
        
        return {"message": "Email updated successfully", "new_email": new_email}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to verify email change")

@app.get("/debug/user/{user_email}")
async def debug_user_storage(user_email: str):
    """Debug endpoint to check user storage data"""
    user_data = get_user_from_firestore(user_email)
    if not user_data:
        return {"error": "User not found"}
    service = get_google_service()
    
    debug_info = {
        "user_email": user_email,
        "user_data": {
            "storage_used": user_data.get('storage_used', 0),
            "total_files": user_data.get('total_files', 0),
            "folder_id": user_data.get('folder_id')
        },
        "google_service_available": service is not None,
        "formatted_storage": get_user_storage_data(user_email)
    }
    
    if service and user_data.get('folder_id'):
        try:
            # Get actual files from Google Drive
            results = service.files().list(
                q=f"'{user_data['folder_id']}' in parents and trashed=false",
                fields="files(id,name,size,mimeType)"
            ).execute()
            files = results.get('files', [])
            
            debug_info["google_drive_files"] = {
                "count": len(files),
                "files": [{
                    "name": f.get('name'),
                    "size": f.get('size', 0),
                    "mimeType": f.get('mimeType')
                } for f in files[:5]]  # Show first 5 files
            }
        except Exception as e:
            debug_info["google_drive_error"] = str(e)
    
    return debug_info

@app.get("/debug/personal-drive/{user_email}")
async def debug_personal_drive(user_email: str, drive_id: str = "drive_2"):
    """Debug endpoint to test personal drive connection"""
    user_tokens = get_user_drive_tokens_from_firestore(user_email)
    debug_info = {
        "user_email": user_email,
        "drive_id": drive_id,
        "has_tokens": drive_id in user_tokens,
        "token_data": None,
        "service_available": False,
        "drive_info": None,
        "error": None
    }
    
    if drive_id in user_tokens:
        debug_info["token_data"] = {
            "has_refresh_token": "refresh_token" in user_tokens[drive_id],
            "expiry": user_tokens[drive_id].get("expiry"),
            "scopes": user_tokens[drive_id].get("scopes", [])
        }
        
        try:
            service = get_user_google_service(user_email, drive_id)
            if service:
                debug_info["service_available"] = True
                
                # Test the service by getting user info
                about = service.about().get(fields="user,storageQuota").execute()
                debug_info["drive_info"] = {
                    "email": about.get('user', {}).get('emailAddress'),
                    "display_name": about.get('user', {}).get('displayName'),
                    "storage_limit": about.get('storageQuota', {}).get('limit'),
                    "storage_usage": about.get('storageQuota', {}).get('usage')
                }
                
                # Test listing files
                files_result = service.files().list(
                    q="'root' in parents and trashed=false",
                    pageSize=5,
                    fields="files(id,name,mimeType)"
                ).execute()
                debug_info["sample_files"] = files_result.get('files', [])
                
        except Exception as e:
            debug_info["error"] = str(e)
    
    return debug_info

@app.get("/test-firestore")
async def test_firestore_connection():
    """Test Firestore database connection"""
    if not db:
        return {
            "firestore_connected": False,
            "error": "Firestore not initialized"
        }
    
    try:
        # Test write
        test_doc = db.collection('connection_test').document('api_test')
        test_data = {
            'timestamp': datetime.utcnow().isoformat(),
            'status': 'testing'
        }
        test_doc.set(test_data)
        
        # Test read
        doc = test_doc.get()
        if doc.exists:
            # Clean up
            test_doc.delete()
            return {
                "firestore_connected": True,
                "test_data": doc.to_dict()
            }
        else:
            return {
                "firestore_connected": False,
                "error": "Document not found after write"
            }
    except Exception as e:
        return {
            "firestore_connected": False,
            "error": str(e)
        }

@app.get("/test-personal-drive")
async def test_personal_drive(current_user: str = Depends(get_current_user), drive_id: str = "drive_2"):
    """Test personal drive connection for current user"""
    service = get_user_google_service(current_user, drive_id)
    
    if not service:
        user_tokens = get_user_drive_tokens_from_firestore(current_user)
        return {
            "connected": False,
            "error": f"No service available for {drive_id}",
            "has_tokens": drive_id in user_tokens
        }
    
    try:
        # Test the connection
        about = service.about().get(fields="user").execute()
        return {
            "connected": True,
            "email": about.get('user', {}).get('emailAddress'),
            "display_name": about.get('user', {}).get('displayName')
        }
    except Exception as e:
        return {
            "connected": False,
            "error": str(e)
        }



@app.get("/debug/user-data/{user_email}")
async def debug_user_firestore_data(user_email: str):
    """Debug endpoint to check what data exists for a user in Firestore"""
    if not db:
        return {"error": "Firestore not available"}
    
    collections_to_check = ['users', 'user_2fa', 'user_drive_tokens', 'email_change_requests']
    user_data = {}
    
    # Check regular collections
    for collection_name in collections_to_check:
        try:
            doc = db.collection(collection_name).document(user_email).get()
            user_data[collection_name] = {
                "exists": doc.exists,
                "data": doc.to_dict() if doc.exists else None
            }
        except Exception as e:
            user_data[collection_name] = {"error": str(e)}
    
    # Check share links
    try:
        shares_ref = db.collection('share_links')
        query = shares_ref.where('owner_email', '==', user_email)
        docs = list(query.stream())
        user_data['share_links'] = {
            "count": len(docs),
            "links": [{
                "id": doc.id,
                "file_name": doc.to_dict().get('file_name'),
                "created_at": doc.to_dict().get('created_at')
            } for doc in docs]
        }
    except Exception as e:
        user_data['share_links'] = {"error": str(e)}
    
    return {
        "user_email": user_email,
        "data": user_data,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/debug/share/{share_token}")
async def debug_share_link(share_token: str):
    """Debug endpoint to check share link status"""
    if not db:
        return {"error": "Database not available"}
    
    try:
        doc = db.collection('share_links').document(share_token).get()
        if not doc.exists:
            return {"error": "Share link not found"}
        
        data = doc.to_dict()
        current_time = datetime.utcnow()
        
        debug_info = {
            "share_token": share_token,
            "file_name": data.get('file_name'),
            "owner_email": data.get('owner_email'),
            "created_at": data.get('created_at'),
            "expires_at": data.get('expires_at'),
            "current_time_utc": current_time.isoformat(),
            "access_count": data.get('access_count', 0),
            "view_limit": data.get('view_limit'),
            "allow_download": data.get('allow_download'),
            "allow_preview": data.get('allow_preview'),
            "use_personal_drive": data.get('use_personal_drive'),
            "drive_id": data.get('drive_id')
        }
        
        # Check time expiry
        time_expired = False
        if data.get('expires_at'):
            try:
                expires_at_str = data['expires_at']
                # Handle both formats: with and without 'Z' suffix
                if expires_at_str.endswith('Z'):
                    expires_at_str = expires_at_str[:-1]
                expires_at = datetime.fromisoformat(expires_at_str)
                debug_info["expires_at_parsed"] = expires_at.isoformat()
                time_expired = current_time >= expires_at
                debug_info["time_until_expiry"] = str(expires_at - current_time) if expires_at > current_time else "Already expired"
            except ValueError as e:
                debug_info["expires_at_error"] = str(e)
                time_expired = True
        else:
            debug_info["time_until_expiry"] = "Never expires"
        
        # Check view limit expiry
        view_limit_expired = False
        view_limit = data.get('view_limit')
        if view_limit is not None:
            access_count = data.get('access_count', 0)
            view_limit_expired = access_count >= view_limit
            debug_info["views_remaining"] = max(0, view_limit - access_count)
            debug_info["view_limit_reached"] = view_limit_expired
        else:
            debug_info["views_remaining"] = "Unlimited"
            debug_info["view_limit_reached"] = False
        
        debug_info["is_expired"] = time_expired or view_limit_expired
        debug_info["expiry_reason"] = []
        if time_expired:
            debug_info["expiry_reason"].append("time")
        if view_limit_expired:
            debug_info["expiry_reason"].append("view_limit")
        
        return debug_info
        
    except Exception as e:
        return {"error": str(e)}





@app.post("/debug/test-cleanup/{user_email}")
async def test_cleanup_for_user(user_email: str):
    """Test cleanup function for a specific user (debug only)"""
    if not db:
        return {"error": "Firestore not available"}
    
    # First check what data exists
    before_data = {}
    collections_to_check = ['users', 'user_2fa', 'user_drive_tokens', 'email_change_requests']
    
    for collection_name in collections_to_check:
        try:
            doc = db.collection(collection_name).document(user_email).get()
            before_data[collection_name] = doc.exists
        except Exception as e:
            before_data[collection_name] = f"error: {str(e)}"
    
    # Check share links
    try:
        shares_ref = db.collection('share_links')
        query = shares_ref.where('owner_email', '==', user_email)
        docs = list(query.stream())
        before_data['share_links'] = len(docs)
    except Exception as e:
        before_data['share_links'] = f"error: {str(e)}"
    
    # Run cleanup
    cleanup_results = cleanup_user_data(user_email)
    
    # Check what data exists after cleanup
    after_data = {}
    for collection_name in collections_to_check:
        try:
            doc = db.collection(collection_name).document(user_email).get()
            after_data[collection_name] = doc.exists
        except Exception as e:
            after_data[collection_name] = f"error: {str(e)}"
    
    # Check share links after
    try:
        shares_ref = db.collection('share_links')
        query = shares_ref.where('owner_email', '==', user_email)
        docs = list(query.stream())
        after_data['share_links'] = len(docs)
    except Exception as e:
        after_data['share_links'] = f"error: {str(e)}"
    
    return {
        "user_email": user_email,
        "before_cleanup": before_data,
        "cleanup_results": cleanup_results,
        "after_cleanup": after_data,
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)



# 2FA Utility Functions
def generate_2fa_secret():
    """Generate a new TOTP secret"""
    return pyotp.random_base32()

def generate_qr_code(user_email: str, secret: str):
    """Generate QR code for TOTP setup"""
    totp_uri = pyotp.totp.TOTP(secret).provisioning_uri(
        name=user_email,
        issuer_name="Novacloud"
    )
    
    qr = qrcode.QRCode(version=1, box_size=10, border=5)
    qr.add_data(totp_uri)
    qr.make(fit=True)
    
    img = qr.make_image(fill_color="black", back_color="white")
    buffer = BytesIO()
    img.save(buffer, format='PNG')
    buffer.seek(0)
    
    return base64.b64encode(buffer.getvalue()).decode()

def generate_backup_codes():
    """Generate backup codes for 2FA"""
    import secrets
    return [f"{secrets.randbelow(100000000):08d}" for _ in range(10)]

def verify_totp_token(secret: str, token: str):
    """Verify TOTP token"""
    totp = pyotp.TOTP(secret)
    return totp.verify(token, valid_window=1)

def get_user_2fa_data(user_email: str):
    """Get user's 2FA data from Firestore"""
    if not db:
        return None
    try:
        doc_ref = db.collection('user_2fa').document(user_email)
        doc = doc_ref.get()
        return doc.to_dict() if doc.exists else None
    except Exception as e:
        print(f"Error getting 2FA data: {str(e)}")
        return None

def save_user_2fa_data(user_email: str, data: dict):
    """Save user's 2FA data to Firestore"""
    if not db:
        return False
    try:
        doc_ref = db.collection('user_2fa').document(user_email)
        doc_ref.set(data)
        return True
    except Exception as e:
        print(f"Error saving 2FA data: {str(e)}")
        return False

# 2FA Endpoints
@app.get("/2fa/status", response_model=TwoFAStatus)
async def get_2fa_status(current_user: str = Depends(get_current_user)):
    """Get 2FA status for current user"""
    twofa_data = get_user_2fa_data(current_user)
    
    if not twofa_data:
        return TwoFAStatus(enabled=False, backup_codes_remaining=0)
    
    backup_codes_remaining = len([code for code in twofa_data.get('backup_codes', []) if not code.get('used', False)])
    
    return TwoFAStatus(
        enabled=twofa_data.get('enabled', False),
        backup_codes_remaining=backup_codes_remaining
    )

@app.post("/2fa/setup", response_model=TwoFASetup)
async def setup_2fa(current_user: str = Depends(get_current_user)):
    """Setup 2FA for current user"""
    # Check if 2FA is already enabled
    existing_data = get_user_2fa_data(current_user)
    if existing_data and existing_data.get('enabled'):
        raise HTTPException(status_code=400, detail="2FA is already enabled")
    
    # Generate new secret and backup codes
    secret = generate_2fa_secret()
    qr_code = generate_qr_code(current_user, secret)
    backup_codes = generate_backup_codes()
    
    # Save temporary 2FA data (not enabled yet)
    twofa_data = {
        'secret': secret,
        'enabled': False,
        'backup_codes': [{'code': code, 'used': False} for code in backup_codes],
        'created_at': datetime.utcnow().isoformat()
    }
    
    if not save_user_2fa_data(current_user, twofa_data):
        raise HTTPException(status_code=500, detail="Failed to save 2FA data")
    
    return TwoFASetup(
        secret=secret,
        qr_code=f"data:image/png;base64,{qr_code}",
        backup_codes=backup_codes
    )

@app.post("/2fa/verify")
async def verify_2fa_setup(
    verify_data: TwoFAVerify,
    current_user: str = Depends(get_current_user)
):
    """Verify and enable 2FA"""
    twofa_data = get_user_2fa_data(current_user)
    
    if not twofa_data:
        raise HTTPException(status_code=400, detail="2FA setup not found")
    
    if twofa_data.get('enabled'):
        raise HTTPException(status_code=400, detail="2FA is already enabled")
    
    # Verify the token
    if not verify_totp_token(twofa_data['secret'], verify_data.token):
        raise HTTPException(status_code=400, detail="Invalid verification code")
    
    # Enable 2FA
    twofa_data['enabled'] = True
    twofa_data['enabled_at'] = datetime.utcnow().isoformat()
    
    if not save_user_2fa_data(current_user, twofa_data):
        raise HTTPException(status_code=500, detail="Failed to enable 2FA")
    
    return {"message": "2FA enabled successfully"}

@app.post("/2fa/verify-login")
async def verify_2fa_login(
    verify_data: TwoFAVerify,
    current_user: str = Depends(get_current_user)
):
    """Verify 2FA token for login"""
    twofa_data = get_user_2fa_data(current_user)
    
    if not twofa_data or not twofa_data.get('enabled'):
        raise HTTPException(status_code=400, detail="2FA is not enabled")
    
    # Check if it's a backup code
    if len(verify_data.token) == 8 and verify_data.token.isdigit():
        backup_codes = twofa_data.get('backup_codes', [])
        for backup_code in backup_codes:
            if backup_code['code'] == verify_data.token and not backup_code['used']:
                # Mark backup code as used
                backup_code['used'] = True
                backup_code['used_at'] = datetime.utcnow().isoformat()
                save_user_2fa_data(current_user, twofa_data)
                return {"message": "2FA verified with backup code", "backup_code_used": True}
        
        raise HTTPException(status_code=400, detail="Invalid or used backup code")
    
    # Verify TOTP token
    if not verify_totp_token(twofa_data['secret'], verify_data.token):
        raise HTTPException(status_code=400, detail="Invalid verification code")
    
    return {"message": "2FA verified successfully", "backup_code_used": False}

@app.post("/2fa/disable")
async def disable_2fa(
    verify_data: TwoFAVerify,
    current_user: str = Depends(get_current_user)
):
    """Disable 2FA"""
    twofa_data = get_user_2fa_data(current_user)
    
    if not twofa_data or not twofa_data.get('enabled'):
        raise HTTPException(status_code=400, detail="2FA is not enabled")
    
    # Verify current token before disabling
    if not verify_totp_token(twofa_data['secret'], verify_data.token):
        raise HTTPException(status_code=400, detail="Invalid verification code")
    
    # Delete 2FA data
    try:
        if db:
            db.collection('user_2fa').document(current_user).delete()
        return {"message": "2FA disabled successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to disable 2FA")

@app.post("/2fa/regenerate-backup-codes")
async def regenerate_backup_codes(
    verify_data: TwoFAVerify,
    current_user: str = Depends(get_current_user)
):
    """Regenerate backup codes"""
    twofa_data = get_user_2fa_data(current_user)
    
    if not twofa_data or not twofa_data.get('enabled'):
        raise HTTPException(status_code=400, detail="2FA is not enabled")
    
    # Verify current token
    if not verify_totp_token(twofa_data['secret'], verify_data.token):
        raise HTTPException(status_code=400, detail="Invalid verification code")
    
    # Generate new backup codes
    new_backup_codes = generate_backup_codes()
    twofa_data['backup_codes'] = [{'code': code, 'used': False} for code in new_backup_codes]
    twofa_data['backup_codes_regenerated_at'] = datetime.utcnow().isoformat()
    
    if not save_user_2fa_data(current_user, twofa_data):
        raise HTTPException(status_code=500, detail="Failed to regenerate backup codes")
    
    return {"backup_codes": new_backup_codes}

@app.post("/delete-account")
async def delete_account(
    password: str = Form(...),
    current_user: str = Depends(get_current_user)
):
    """Delete user account and all associated data"""
    try:
        # Get Firebase user
        firebase_user = auth.get_user_by_email(current_user)
        
        # Get user data from Firestore
        user_data = get_user_from_firestore(current_user)
        
        # Delete shared folder and files from Google Drive
        service = get_google_service()
        if service and user_data:
            user_folder_id = user_data.get('folder_id')
            if user_folder_id:
                try:
                    service.files().delete(fileId=user_folder_id).execute()
                    print(f"Deleted user folder: {user_folder_id}")
                except Exception as e:
                    print(f"Error deleting user folder: {str(e)}")
        
        # Delete from ALL Firestore collections using batch deletion
        if db:
            print(f"Starting Firestore cleanup for {current_user}")
            cleanup_results = cleanup_user_data(current_user)
            print(f"Cleanup results: {cleanup_results}")
        
        # Delete Firebase user last
        auth.delete_user(firebase_user.uid)
        print(f"Deleted Firebase user: {firebase_user.uid}")
        
        return {
            "message": "Account deleted successfully",
            "cleanup_results": cleanup_results if 'cleanup_results' in locals() else "No cleanup performed"
        }
        
    except Exception as e:
        print(f"Account deletion error: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to delete account")

# Share Link Models
class ShareLinkRequest(BaseModel):
    file_id: str
    file_name: str
    expiry_hours: Optional[int] = 24
    allow_download: bool = True
    allow_preview: bool = True
    use_personal_drive: bool = False
    drive_id: str = "drive_1"
    view_limit: Optional[int] = None  # None means unlimited views

class EmailShareRequest(BaseModel):
    file_id: str
    file_name: str
    recipient_email: str
    expiry_hours: Optional[int] = 24
    use_personal_drive: bool = False
    drive_id: str = "drive_1"

class ShareRequest(BaseModel):
    recipient_email: str





@app.post("/share/email")
async def share_via_email(request: EmailShareRequest, current_user: str = Depends(get_current_user)):
    """Share file via email to another NovaCloud user"""
    if not db:
        raise HTTPException(status_code=500, detail="Database not available")
    
    # Check if recipient exists in NovaCloud
    try:
        auth.get_user_by_email(request.recipient_email)
    except:
        raise HTTPException(status_code=404, detail="Recipient not found in NovaCloud")
    
    if request.recipient_email == current_user:
        raise HTTPException(status_code=400, detail="Cannot share with yourself")
    
    # Calculate expiry time
    expires_at = None
    if request.expiry_hours and request.expiry_hours > 0:
        expires_at = datetime.utcnow() + timedelta(hours=request.expiry_hours)
    
    try:
        share_data = {
            'file_id': request.file_id,
            'file_name': request.file_name,
            'sender_email': current_user,
            'recipient_email': request.recipient_email,
            'expires_at': expires_at.isoformat() + 'Z' if expires_at else None,
            'created_at': datetime.utcnow().isoformat() + 'Z',
            'use_personal_drive': request.use_personal_drive,
            'drive_id': request.drive_id,
            'status': 'active'
        }
        
        doc_ref = db.collection('email_shares').add(share_data)
        
        return {
            "message": f"File shared with {request.recipient_email} successfully",
            "share_id": doc_ref[1].id
        }
        
    except Exception as e:
        print(f"Error sharing via email: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to share file")

@app.post("/share/generate-link")
async def generate_share_link(request: ShareLinkRequest, current_user: str = Depends(get_current_user)):
    share_token = secrets.token_urlsafe(16)
    
    # Calculate expiry time properly with UTC timezone
    expires_at = None
    if request.expiry_hours and request.expiry_hours > 0:
        # Use exact hours without buffer to match user expectation
        expires_at = datetime.utcnow() + timedelta(hours=request.expiry_hours)
    
    if db:
        share_data = {
            'file_id': request.file_id,
            'file_name': request.file_name,
            'owner_email': current_user,
            'allow_download': request.allow_download,
            'allow_preview': request.allow_preview,
            'expires_at': expires_at.isoformat() + 'Z' if expires_at else None,  # Add Z for UTC
            'access_count': 0,
            'view_limit': request.view_limit,
            'created_at': datetime.utcnow().isoformat() + 'Z',  # Add Z for UTC
            'use_personal_drive': request.use_personal_drive,
            'drive_id': request.drive_id
        }
        db.collection('share_links').document(share_token).set(share_data)
    
    return {"share_url": f"{os.getenv('FRONTEND_URL', 'https://novacloud22.web.app')}/share/{share_token}"}

@app.get("/share/my-links")
async def get_user_shared_links(current_user: str = Depends(get_current_user)):
    """Get all shared links created by the current user"""
    if not db:
        raise HTTPException(status_code=500, detail="Database not available")
    
    try:
        links_ref = db.collection('share_links')
        query = links_ref.where('owner_email', '==', current_user)
        docs = list(query.stream())
        
        links = []
        for doc in docs:
            link_data = doc.to_dict()
            link_data['share_token'] = doc.id
            link_data['share_url'] = f"{os.getenv('FRONTEND_URL', 'https://novacloud22.web.app')}/share/{doc.id}"
            links.append(link_data)
        
        links.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        return {"links": links}
    except Exception as e:
        print(f"Error fetching user shared links: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch shared links")

@app.get("/share/analytics")
async def get_share_analytics(current_user: str = Depends(get_current_user)):
    """Get sharing analytics for current user"""
    if not db:
        raise HTTPException(status_code=500, detail="Database not available")
    
    try:
        # Get all share links created by current user
        shares_ref = db.collection('share_links')
        query = shares_ref.where('owner_email', '==', current_user)
        docs = list(query.stream())
        
        total_shares = len(docs)
        total_access_count = 0
        active_shares = 0
        expired_shares = 0
        personal_drive_shares = 0
        shared_drive_shares = 0
        
        current_time = datetime.utcnow()
        
        for doc in docs:
            data = doc.to_dict()
            total_access_count += data.get('access_count', 0)
            
            # Check if expired
            if data.get('expires_at'):
                expires_at = datetime.fromisoformat(data['expires_at'])
                if current_time > expires_at:
                    expired_shares += 1
                else:
                    active_shares += 1
            else:
                active_shares += 1
            
            # Count drive types
            if data.get('use_personal_drive', False):
                personal_drive_shares += 1
            else:
                shared_drive_shares += 1
        
        return {
            "total_shares": total_shares,
            "active_shares": active_shares,
            "expired_shares": expired_shares,
            "total_access_count": total_access_count,
            "personal_drive_shares": personal_drive_shares,
            "shared_drive_shares": shared_drive_shares
        }
        
    except Exception as e:
        print(f"Error getting share analytics: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get analytics")

@app.get("/share/list")
async def list_user_shares(current_user: str = Depends(get_current_user)):
    """List all share links created by current user"""
    if not db:
        raise HTTPException(status_code=500, detail="Database not available")
    
    try:
        shares_ref = db.collection('share_links')
        query = shares_ref.where('owner_email', '==', current_user)
        docs = list(query.stream())
        
        shares = []
        current_time = datetime.utcnow()
        
        for doc in docs:
            data = doc.to_dict()
            share_token = doc.id
            
            # Check if expired by time
            is_expired = False
            if data.get('expires_at'):
                expires_at = datetime.fromisoformat(data['expires_at'])
                is_expired = current_time > expires_at
            
            # Check if expired by view limit
            view_limit_expired = False
            view_limit = data.get('view_limit')
            if view_limit is not None:
                view_limit_expired = data.get('access_count', 0) >= view_limit
            
            shares.append({
                "token": share_token,
                "file_name": data.get('file_name'),
                "file_id": data.get('file_id'),
                "created_at": data.get('created_at'),
                "expires_at": data.get('expires_at'),
                "is_expired": is_expired or view_limit_expired,
                "access_count": data.get('access_count', 0),
                "view_limit": view_limit,
                "allow_download": data.get('allow_download', True),
                "allow_preview": data.get('allow_preview', True),
                "use_personal_drive": data.get('use_personal_drive', False),
                "drive_id": data.get('drive_id', 'drive_1'),
                "share_url": f"{os.getenv('FRONTEND_URL', 'https://novacloud22.web.app')}/share/{share_token}"
            })
        
        # Sort by creation date (newest first)
        shares.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        
        return {"shares": shares}
        
    except Exception as e:
        print(f"Error listing shares: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to list shares")

@app.get("/share/{share_token}")
async def access_shared_file(share_token: str):
    if not db: raise HTTPException(status_code=500, detail="Database not available")
    doc = db.collection('share_links').document(share_token).get()
    if not doc.exists: raise HTTPException(status_code=404, detail="Share link not found")
    data = doc.to_dict()
    
    # Check if link is expired by time
    if data.get('expires_at'):
        try:
            expires_at_str = data['expires_at']
            # Handle both formats: with and without 'Z' suffix
            if expires_at_str.endswith('Z'):
                expires_at_str = expires_at_str[:-1]
            expires_at = datetime.fromisoformat(expires_at_str)
            current_time = datetime.utcnow()
            if current_time >= expires_at:
                raise HTTPException(status_code=410, detail="Share link expired")
        except ValueError:
            raise HTTPException(status_code=410, detail="Share link expired")
    
    # Check if view limit is reached
    current_views = data.get('access_count', 0)
    view_limit = data.get('view_limit')
    if view_limit is not None and current_views >= view_limit:
        raise HTTPException(status_code=410, detail="Share link expired - view limit reached")
    
    # Get appropriate service to check if it's a folder
    if data.get('use_personal_drive', False):
        service = get_user_google_service(data['owner_email'], data.get('drive_id', 'drive_1'))
    else:
        service = get_google_service()
    
    folder_contents = None
    is_folder = False
    
    if service:
        try:
            # Get file metadata to check if it's a folder
            file_metadata = service.files().get(fileId=data['file_id'], fields='id,name,mimeType').execute()
            is_folder = file_metadata.get('mimeType') == 'application/vnd.google-apps.folder'
            
            # If it's a folder, get its contents
            if is_folder:
                results = service.files().list(
                    q=f"'{data['file_id']}' in parents and trashed=false",
                    fields="files(id,name,size,mimeType,createdTime,webViewLink)",
                    pageSize=1000
                ).execute()
                folder_contents = results.get('files', [])
        except Exception as e:
            print(f"Error checking folder contents: {str(e)}")
    
    # Update access count
    doc.reference.update({'access_count': current_views + 1})
    
    response_data = {
        'file_id': data['file_id'], 
        'file_name': data['file_name'], 
        'allow_download': data['allow_download'], 
        'allow_preview': data['allow_preview'],
        'view_limit': view_limit,
        'views_remaining': view_limit - (current_views + 1) if view_limit else None,
        'is_folder': is_folder
    }
    
    if is_folder and folder_contents is not None:
        response_data['folder_contents'] = folder_contents
    
    return response_data

@app.get("/share/{share_token}/preview")
async def preview_shared_file(share_token: str, file_id: Optional[str] = None):
    if not db: raise HTTPException(status_code=500, detail="Database not available")
    doc = db.collection('share_links').document(share_token).get()
    if not doc.exists: raise HTTPException(status_code=404, detail="Share link not found")
    data = doc.to_dict()
    
    # Check if link is expired by time
    if data.get('expires_at'):
        try:
            expires_at_str = data['expires_at']
            if expires_at_str.endswith('Z'):
                expires_at_str = expires_at_str[:-1]
            expires_at = datetime.fromisoformat(expires_at_str)
            current_time = datetime.utcnow()
            if current_time >= expires_at:
                raise HTTPException(status_code=410, detail="Share link expired")
        except ValueError:
            raise HTTPException(status_code=410, detail="Share link expired")
    
    # Check if view limit is reached
    current_views = data.get('access_count', 0)
    view_limit = data.get('view_limit')
    if view_limit is not None and current_views >= view_limit:
        raise HTTPException(status_code=410, detail="Share link expired - view limit reached")
    
    if not data.get('allow_preview', True): 
        raise HTTPException(status_code=403, detail="Preview not allowed")
    
    # Get appropriate service based on drive type
    if data.get('use_personal_drive', False):
        service = get_user_google_service(data['owner_email'], data.get('drive_id', 'drive_1'))
    else:
        service = get_google_service()
    
    if not service: 
        raise HTTPException(status_code=500, detail="Service unavailable")
    
    # Use provided file_id if available (for files within shared folder), otherwise use original file_id
    target_file_id = file_id if file_id else data['file_id']
    
    # Only verify folder permissions if file_id is provided (accessing file within shared folder)
    if file_id:
        try:
            # Check if the original shared item is a folder
            original_metadata = service.files().get(fileId=data['file_id'], fields='mimeType').execute()
            if original_metadata.get('mimeType') != 'application/vnd.google-apps.folder':
                raise HTTPException(status_code=403, detail="File access not allowed")
            
            # Verify the requested file is within the shared folder
            file_metadata = service.files().get(fileId=file_id, fields='parents').execute()
            if data['file_id'] not in file_metadata.get('parents', []):
                raise HTTPException(status_code=403, detail="File not in shared folder")
        except HttpError as e:
            if e.resp.status == 404:
                raise HTTPException(status_code=404, detail="File not found")
            raise HTTPException(status_code=500, detail="Error accessing file")
        except Exception as e:
            if "not in shared folder" in str(e) or "File access not allowed" in str(e):
                raise
            raise HTTPException(status_code=404, detail="File not found")
    
    try:
        file_metadata = service.files().get(fileId=target_file_id, fields='id,name,mimeType,size').execute()
    except HttpError as e:
        if e.resp.status == 404:
            raise HTTPException(status_code=404, detail="File not found")
        elif e.resp.status == 403:
            raise HTTPException(status_code=403, detail="Access denied to file")
        else:
            raise HTTPException(status_code=500, detail="Error accessing file")
    except Exception as e:
        raise HTTPException(status_code=404, detail="File not found")
    
    file_name = file_metadata.get('name', '').lower()
    mime_type = file_metadata.get('mimeType', 'application/octet-stream')
    
    # Enhanced file type handling for preview
    if mime_type == 'text/csv' or file_name.endswith('.csv'):
        mime_type = 'text/plain; charset=utf-8'
    elif file_name.endswith(('.py', '.js', '.jsx', '.ts', '.tsx', '.css', '.html', '.htm', '.txt', '.md', '.json', '.xml', '.yaml', '.yml', '.sql', '.php', '.rb', '.go', '.rs', '.kt', '.swift', '.dart', '.sh', '.bat', '.ps1')):
        mime_type = 'text/plain; charset=utf-8'
    elif mime_type.startswith('image/') or file_name.endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.ico', '.tiff', '.tif')):
        if file_name.endswith('.svg'):
            mime_type = 'image/svg+xml'
        elif file_name.endswith(('.jpg', '.jpeg')):
            mime_type = 'image/jpeg'
        elif file_name.endswith('.png'):
            mime_type = 'image/png'
        elif file_name.endswith('.gif'):
            mime_type = 'image/gif'
        elif file_name.endswith('.webp'):
            mime_type = 'image/webp'
    elif 'pdf' in mime_type or file_name.endswith('.pdf'):
        mime_type = 'application/pdf'
    
    # Quick validation check before streaming
    try:
        # Just verify the file exists and we have access
        file_info = service.files().get(fileId=target_file_id, fields='id,name').execute()
        print(f"Validated file access for preview: {file_info.get('name')} (ID: {target_file_id})")
    except HttpError as e:
        print(f"Google Drive validation error for file {target_file_id}: {e.resp.status} - {str(e)}")
        if e.resp.status == 404:
            raise HTTPException(status_code=404, detail="File not found or has been deleted")
        elif e.resp.status == 403:
            raise HTTPException(status_code=403, detail="Access denied to file")
        elif e.resp.status == 429:
            raise HTTPException(status_code=429, detail="Rate limit exceeded. Please try again later.")
        else:
            raise HTTPException(status_code=500, detail=f"Preview failed: Google Drive error {e.resp.status}")
    except Exception as e:
        print(f"Validation error for file {target_file_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Preview failed: {str(e)}")
    
    def generate_stream():
        request = None
        downloader = None
        file_io = None
        
        try:
            request = service.files().get_media(fileId=target_file_id)
            file_io = io.BytesIO()
            downloader = MediaIoBaseDownload(file_io, request, chunksize=1024*1024)
            
            done = False
            while not done:
                try:
                    status, done = downloader.next_chunk()
                    if status:
                        file_io.seek(0)
                        chunk = file_io.read()
                        if chunk:
                            yield chunk
                        file_io.seek(0)
                        file_io.truncate(0)
                except Exception as chunk_error:
                    print(f"Error during chunk preview for {target_file_id}: {str(chunk_error)}")
                    # Stop streaming on chunk error
                    break
                    
        except HttpError as e:
            print(f"Google Drive API error streaming file {target_file_id}: {str(e)}")
            # For streaming, we can't raise HTTPException, so we yield an error response
            error_msg = f"Preview failed: {e.resp.status}"
            if e.resp.status == 404:
                error_msg = "File not found or has been deleted"
            elif e.resp.status == 403:
                error_msg = "Access denied to file"
            elif e.resp.status == 429:
                error_msg = "Rate limit exceeded. Please try again later."
            
            # Yield a minimal error response that browsers will handle
            yield error_msg.encode('utf-8')
            
        except Exception as e:
            print(f"Error streaming file {target_file_id}: {str(e)}")
            yield f"Preview failed: {str(e)}".encode('utf-8')
            
        finally:
            # Clean up resources
            if file_io:
                try:
                    file_io.close()
                except:
                    pass
    
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'public, max-age=3600',
        'Content-Disposition': 'inline'
    }
    
    if mime_type.startswith('video/') or mime_type.startswith('audio/'):
        headers['Accept-Ranges'] = 'bytes'
    
    return StreamingResponse(
        generate_stream(),
        media_type=mime_type,
        headers=headers
    )

@app.get("/share/{share_token}/download")
async def download_shared_file(share_token: str, file_id: Optional[str] = None):
    if not db: 
        raise HTTPException(status_code=500, detail="Database not available")
    
    doc = db.collection('share_links').document(share_token).get()
    if not doc.exists: 
        raise HTTPException(status_code=404, detail="Share link not found")
    
    data = doc.to_dict()
    
    # Check if link is expired by time
    if data.get('expires_at'):
        try:
            expires_at_str = data['expires_at']
            if expires_at_str.endswith('Z'):
                expires_at_str = expires_at_str[:-1]
            expires_at = datetime.fromisoformat(expires_at_str)
            current_time = datetime.utcnow()
            if current_time >= expires_at:
                raise HTTPException(status_code=410, detail="Share link expired")
        except ValueError:
            raise HTTPException(status_code=410, detail="Share link expired")
    
    # Check if view limit is reached
    current_views = data.get('access_count', 0)
    view_limit = data.get('view_limit')
    if view_limit is not None and current_views >= view_limit:
        raise HTTPException(status_code=410, detail="Share link expired - view limit reached")
    
    if not data.get('allow_download', True): 
        raise HTTPException(status_code=403, detail="Download not allowed")
    
    # Get appropriate service based on drive type
    if data.get('use_personal_drive', False):
        service = get_user_google_service(data['owner_email'], data.get('drive_id', 'drive_1'))
    else:
        service = get_google_service()
    
    if not service: 
        raise HTTPException(status_code=500, detail="Service unavailable")
    
    # Use provided file_id if available (for files within shared folder), otherwise use original file_id
    target_file_id = file_id if file_id else data['file_id']
    
    # Only verify folder permissions if file_id is provided (accessing file within shared folder)
    if file_id:
        try:
            # Check if the original shared item is a folder
            original_metadata = service.files().get(fileId=data['file_id'], fields='mimeType').execute()
            if original_metadata.get('mimeType') != 'application/vnd.google-apps.folder':
                raise HTTPException(status_code=403, detail="File access not allowed")
            
            # Verify the requested file is within the shared folder
            file_metadata = service.files().get(fileId=file_id, fields='parents').execute()
            if data['file_id'] not in file_metadata.get('parents', []):
                raise HTTPException(status_code=403, detail="File not in shared folder")
        except HttpError as e:
            if e.resp.status == 404:
                raise HTTPException(status_code=404, detail="File not found")
            raise HTTPException(status_code=500, detail="Error accessing file")
        except Exception as e:
            if "not in shared folder" in str(e) or "File access not allowed" in str(e):
                raise
            raise HTTPException(status_code=404, detail="File not found")
    
    try:
        file_metadata = service.files().get(fileId=target_file_id, fields='id,name,mimeType,size').execute()
    except HttpError as e:
        if e.resp.status == 404:
            raise HTTPException(status_code=404, detail="File not found")
        elif e.resp.status == 403:
            raise HTTPException(status_code=403, detail="Access denied to file")
        else:
            raise HTTPException(status_code=500, detail="Error accessing file")
    except Exception as e:
        raise HTTPException(status_code=404, detail="File not found")
    
    def generate_stream():
        request = service.files().get_media(fileId=target_file_id)
        file_io = io.BytesIO()
        file_size = int(file_metadata.get('size', 0))
        chunk_size = get_optimal_chunk_size(file_size)
        downloader = MediaIoBaseDownload(file_io, request, chunksize=chunk_size)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                file_io.seek(0)
                chunk = file_io.read()
                if chunk:
                    yield chunk
                file_io.seek(0)
                file_io.truncate(0)
    
    # Get original filename and preserve extension
    filename = file_metadata.get('name', 'file')
    file_extension = filename.lower().split('.')[-1] if '.' in filename else ''
    
    # Comprehensive MIME type mapping
    mime_types = {
        # Images
        'png': 'image/png', 'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'gif': 'image/gif',
        'bmp': 'image/bmp', 'webp': 'image/webp', 'svg': 'image/svg+xml', 'ico': 'image/x-icon',
        'tiff': 'image/tiff', 'tif': 'image/tiff',
        # Documents
        'pdf': 'application/pdf', 'txt': 'text/plain', 'csv': 'text/csv', 'md': 'text/markdown',
        'json': 'application/json', 'xml': 'application/xml', 'html': 'text/html', 'htm': 'text/html',
        'css': 'text/css', 'js': 'application/javascript', 'jsx': 'text/jsx', 'ts': 'text/typescript',
        'tsx': 'text/tsx', 'py': 'text/x-python', 'java': 'text/x-java-source', 'c': 'text/x-c',
        'cpp': 'text/x-c++', 'h': 'text/x-c', 'hpp': 'text/x-c++', 'php': 'text/x-php',
        'rb': 'text/x-ruby', 'go': 'text/x-go', 'rs': 'text/x-rust', 'kt': 'text/x-kotlin',
        'swift': 'text/x-swift', 'dart': 'text/x-dart', 'sh': 'text/x-shellscript',
        'bat': 'text/x-msdos-batch', 'ps1': 'text/x-powershell', 'sql': 'text/x-sql',
        'yaml': 'text/yaml', 'yml': 'text/yaml', 'toml': 'text/x-toml', 'ini': 'text/plain',
        'cfg': 'text/plain', 'conf': 'text/plain', 'log': 'text/plain',
        # Office documents
        'doc': 'application/msword', 'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'xls': 'application/vnd.ms-excel', 'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'ppt': 'application/vnd.ms-powerpoint', 'pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
        # Archives
        'zip': 'application/zip', 'rar': 'application/x-rar-compressed', '7z': 'application/x-7z-compressed',
        'tar': 'application/x-tar', 'gz': 'application/gzip', 'bz2': 'application/x-bzip2',
        'xz': 'application/x-xz',
        # Videos
        'mp4': 'video/mp4', 'avi': 'video/x-msvideo', 'mov': 'video/quicktime', 'wmv': 'video/x-ms-wmv',
        'flv': 'video/x-flv', 'webm': 'video/webm', 'mkv': 'video/x-matroska', 'm4v': 'video/mp4',
        '3gp': 'video/3gpp', 'ogv': 'video/ogg',
        # Audio
        'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'ogg': 'audio/ogg', 'aac': 'audio/aac',
        'flac': 'audio/flac', 'm4a': 'audio/mp4', 'wma': 'audio/x-ms-wma',
        # Executables
        'exe': 'application/x-msdownload', 'msi': 'application/x-msi', 'dmg': 'application/x-apple-diskimage',
        'deb': 'application/x-debian-package', 'rpm': 'application/x-rpm', 'apk': 'application/vnd.android.package-archive',
        'iso': 'application/x-iso9660-image'
    }
    proper_mime_type = mime_types.get(file_extension, 'application/octet-stream')
    
    # Sanitize filename for HTTP headers (Latin-1 encoding)
    import re
    safe_filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove Unicode characters that can't be encoded in Latin-1
    safe_filename = safe_filename.encode('ascii', 'ignore').decode('ascii')
    if not safe_filename or safe_filename.strip() == '':
        safe_filename = f'file.{file_extension}' if file_extension else 'file'
    
    return StreamingResponse(
        generate_stream(),
        media_type=proper_mime_type,
        headers={
            "Content-Disposition": f'attachment; filename="{safe_filename}"',
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-cache"
        }
    )

@app.post("/share/{share_token}/expire")
async def expire_shared_link(share_token: str, current_user: str = Depends(get_current_user)):
    """Expire a shared link"""
    if not db:
        raise HTTPException(status_code=500, detail="Database not available")
    
    try:
        doc_ref = db.collection('share_links').document(share_token)
        doc = doc_ref.get()
        
        if not doc.exists:
            raise HTTPException(status_code=404, detail="Share link not found")
        
        link_data = doc.to_dict()
        if link_data.get('owner_email') != current_user:
            raise HTTPException(status_code=403, detail="Not authorized to modify this link")
        
        doc_ref.update({
            'expires_at': datetime.utcnow().isoformat(),
            'expired_by_user': True
        })
        
        return {"message": "Link expired successfully"}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error expiring shared link: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to expire link")

@app.delete("/share/{share_token}")
async def delete_share_link(share_token: str, current_user: str = Depends(get_current_user)):
    """Delete a share link"""
    if not db:
        raise HTTPException(status_code=500, detail="Database not available")
    
    try:
        doc_ref = db.collection('share_links').document(share_token)
        doc = doc_ref.get()
        
        if not doc.exists:
            raise HTTPException(status_code=404, detail="Share link not found")
        
        data = doc.to_dict()
        if data.get('owner_email') != current_user:
            raise HTTPException(status_code=403, detail="Not authorized to delete this share link")
        
        doc_ref.delete()
        return {"message": "Share link deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error deleting share link: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to delete share link")

@app.post("/share/email")
async def share_via_email(request: EmailShareRequest, current_user: str = Depends(get_current_user)):
    """Share file via email to another NovaCloud user"""
    if not db:
        raise HTTPException(status_code=500, detail="Database not available")
    
    # Check if recipient exists in NovaCloud
    try:
        auth.get_user_by_email(request.recipient_email)
    except Exception as e:
        raise HTTPException(status_code=404, detail="User not found in NovaCloud")
    
    if request.recipient_email == current_user:
        raise HTTPException(status_code=400, detail="Cannot share with yourself")
    
    # Calculate expiry time
    expires_at = None
    if request.expiry_hours and request.expiry_hours > 0:
        expires_at = datetime.utcnow() + timedelta(hours=request.expiry_hours, minutes=1)
    
    try:
        share_data = {
            'file_id': request.file_id,
            'file_name': request.file_name,
            'sender_email': current_user,
            'recipient_email': request.recipient_email,
            'expires_at': expires_at.isoformat() + 'Z' if expires_at else None,
            'created_at': datetime.utcnow().isoformat() + 'Z',
            'use_personal_drive': request.use_personal_drive,
            'drive_id': request.drive_id,
            'status': 'active'
        }
        
        doc_ref = db.collection('email_shares').add(share_data)
        
        return {
            "message": f"File shared with {request.recipient_email} successfully",
            "share_id": doc_ref[1].id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error sharing via email: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to share file")

# Shared Files Endpoints
@app.get("/user/check-exists")
async def check_user_exists_endpoint(email: str):
    """Check if user exists in NovaCloud"""
    try:
        auth.get_user_by_email(email)
        return {"exists": True}
    except:
        return {"exists": False}

@app.get("/shared/with-me")
async def get_shared_with_me(current_user: str = Depends(get_current_user)):
    """Get files shared with current user via email"""
    if not db:
        raise HTTPException(status_code=500, detail="Database not available")
    
    try:
        shares_ref = db.collection('email_shares')
        query = shares_ref.where('recipient_email', '==', current_user)
        docs = list(query.stream())
        
        shares = []
        current_time = datetime.utcnow()
        
        for doc in docs:
            share_data = doc.to_dict()
            share_data['id'] = doc.id
            
            # Skip expired or deleted shares
            if share_data.get('status') == 'expired':
                continue
                
            # Check if share has expired by time
            if share_data.get('expires_at'):
                try:
                    expires_at = datetime.fromisoformat(share_data['expires_at'].replace('Z', ''))
                    if current_time > expires_at:
                        continue
                except ValueError:
                    continue
            
            shares.append(share_data)
        
        shares.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        return shares
        
    except Exception as e:
        print(f"Error getting shared with me: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get shared files")

@app.get("/shared/by-me")
async def get_shared_by_me(current_user: str = Depends(get_current_user)):
    """Get files shared by current user via email"""
    if not db:
        raise HTTPException(status_code=500, detail="Database not available")
    
    try:
        shares_ref = db.collection('email_shares')
        query = shares_ref.where('sender_email', '==', current_user)
        docs = list(query.stream())
        
        shares = []
        current_time = datetime.utcnow()
        
        for doc in docs:
            share_data = doc.to_dict()
            share_data['id'] = doc.id
            
            # Skip expired or deleted shares
            if share_data.get('status') == 'expired':
                continue
                
            # Check if share has expired by time
            if share_data.get('expires_at'):
                try:
                    expires_at = datetime.fromisoformat(share_data['expires_at'].replace('Z', ''))
                    if current_time > expires_at:
                        continue
                except ValueError:
                    continue
            
            shares.append(share_data)
        
        shares.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        return shares
        
    except Exception as e:
        print(f"Error getting shared by me: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get shared files")

@app.get("/shared/email/{share_id}")
async def access_shared_email_file(share_id: str):
    """Access a file shared via email - public endpoint without authentication"""
    if not db:
        raise HTTPException(status_code=500, detail="Database not available")
    
    try:
        doc_ref = db.collection('email_shares').document(share_id)
        doc = doc_ref.get()
        
        if not doc.exists:
            raise HTTPException(status_code=404, detail="Share not found")
        
        share_data = doc.to_dict()
        
        # Check if share is active and not expired
        if share_data.get('status') != 'active':
            raise HTTPException(status_code=403, detail="Share is not active")
        
        if share_data.get('expires_at'):
            expires_at = datetime.fromisoformat(share_data['expires_at'].replace('Z', ''))
            if datetime.utcnow() > expires_at:
                raise HTTPException(status_code=410, detail="Share has expired")
        
        # Get appropriate service to check if it's a folder
        if share_data.get('use_personal_drive', False):
            service = get_user_google_service(share_data['sender_email'], share_data.get('drive_id', 'drive_1'))
        else:
            service = get_google_service()
        
        is_folder = False
        if service:
            try:
                file_metadata = service.files().get(fileId=share_data['file_id'], fields='mimeType').execute()
                is_folder = file_metadata.get('mimeType') == 'application/vnd.google-apps.folder'
            except Exception as e:
                print(f"Error checking file type: {str(e)}")
        
        return {
            'file_id': share_data['file_id'],
            'file_name': share_data['file_name'],
            'allow_download': True,  # Email shares allow download by default
            'allow_preview': not is_folder,  # Only allow preview for files, not folders
            'sender_email': share_data.get('sender_email'),
            'is_folder': is_folder
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error accessing shared email file: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to access shared file")

@app.get("/shared/email/{share_id}/preview")
async def preview_shared_email_file(share_id: str, current_user: Optional[str] = Depends(get_optional_current_user)):
    """Preview a file shared via email - accessible without authentication for public access"""
    if not db:
        raise HTTPException(status_code=500, detail="Database not available")
    
    try:
        doc_ref = db.collection('email_shares').document(share_id)
        doc = doc_ref.get()
        
        if not doc.exists:
            raise HTTPException(status_code=404, detail="Share not found")
        
        share_data = doc.to_dict()
        
        # If user is authenticated, check authorization (sender or recipient)
        if current_user:
            if share_data.get('sender_email') != current_user and share_data.get('recipient_email') != current_user:
                raise HTTPException(status_code=403, detail="Not authorized to access this share")
        
        # Check if share is active and not expired
        if share_data.get('status') != 'active':
            raise HTTPException(status_code=403, detail="Share is not active")
        
        if share_data.get('expires_at'):
            expires_at = datetime.fromisoformat(share_data['expires_at'].replace('Z', ''))
            if datetime.utcnow() > expires_at:
                raise HTTPException(status_code=410, detail="Share has expired")
        
        # Get appropriate service
        if share_data.get('use_personal_drive', False):
            service = get_user_google_service(share_data['sender_email'], share_data.get('drive_id', 'drive_1'))
        else:
            service = get_google_service()
        
        if not service:
            raise HTTPException(status_code=500, detail="Service unavailable")
        
        # Get file metadata and check if it's a folder
        file_id = share_data['file_id']
        file_metadata = service.files().get(fileId=file_id, fields='id,name,mimeType,size').execute()
        
        # If it's a folder, return an error instead of trying to stream it
        if file_metadata.get('mimeType') == 'application/vnd.google-apps.folder':
            raise HTTPException(status_code=400, detail="Cannot preview folders. Use download to get folder contents as ZIP.")
        
        def generate_stream():
            request = service.files().get_media(fileId=file_id)
            file_io = io.BytesIO()
            downloader = MediaIoBaseDownload(file_io, request, chunksize=1024*1024)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    file_io.seek(0)
                    chunk = file_io.read()
                    if chunk:
                        yield chunk
                    file_io.seek(0)
                    file_io.truncate(0)
        
        # Enhanced file type handling for preview
        file_name = file_metadata.get('name', '').lower()
        mime_type = file_metadata.get('mimeType', 'application/octet-stream')
        
        # Get file extension
        file_ext = file_name.split('.')[-1] if '.' in file_name else ''
        
        # Text files
        if file_ext in ['txt', 'csv', 'log', 'md', 'py', 'js', 'jsx', 'ts', 'tsx', 'css', 'html', 'htm', 'json', 'xml', 'yaml', 'yml', 'sql', 'php', 'rb', 'go', 'rs', 'kt', 'swift', 'dart', 'sh', 'bat', 'ps1', 'c', 'cpp', 'h', 'hpp', 'java', 'scala', 'r', 'lua', 'm', 'pl', 'ini', 'cfg', 'conf', 'toml']:
            mime_type = 'text/plain; charset=utf-8'
        # Images
        elif file_ext in ['jpg', 'jpeg']:
            mime_type = 'image/jpeg'
        elif file_ext == 'png':
            mime_type = 'image/png'
        elif file_ext == 'gif':
            mime_type = 'image/gif'
        elif file_ext == 'svg':
            mime_type = 'image/svg+xml'
        elif file_ext in ['bmp', 'webp', 'ico', 'tiff', 'tif']:
            mime_type = f'image/{file_ext}'
        # Documents
        elif file_ext == 'pdf':
            mime_type = 'application/pdf'
        # Videos
        elif file_ext in ['mp4', 'avi', 'mov', 'wmv', 'flv', 'webm', 'mkv', 'm4v', '3gp', 'ogv']:
            mime_type = f'video/{"mp4" if file_ext == "m4v" else file_ext}'
        # Audio
        elif file_ext in ['mp3', 'wav', 'ogg', 'aac', 'flac', 'm4a', 'wma']:
            mime_type = f'audio/{"mpeg" if file_ext == "mp3" else file_ext}'
        
        headers = {
            'Content-Disposition': 'inline',
            'Cache-Control': 'public, max-age=3600',
            'Access-Control-Allow-Origin': '*'
        }
        
        if mime_type.startswith('video/') or mime_type.startswith('audio/'):
            headers['Accept-Ranges'] = 'bytes'
        
        return StreamingResponse(
            generate_stream(),
            media_type=mime_type,
            headers=headers
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error previewing shared file: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to preview file")

@app.get("/shared/email/{share_id}/download")
async def download_shared_email_file(share_id: str, current_user: Optional[str] = Depends(get_optional_current_user)):
    """Download a file shared via email - accessible without authentication for public access"""
    if not db:
        raise HTTPException(status_code=500, detail="Database not available")
    
    try:
        doc_ref = db.collection('email_shares').document(share_id)
        doc = doc_ref.get()
        
        if not doc.exists:
            raise HTTPException(status_code=404, detail="Share not found")
        
        share_data = doc.to_dict()
        
        # If user is authenticated, check authorization (sender or recipient)
        if current_user:
            if share_data.get('sender_email') != current_user and share_data.get('recipient_email') != current_user:
                raise HTTPException(status_code=403, detail="Not authorized to access this share")
        
        # Check if share is active and not expired
        if share_data.get('status') != 'active':
            raise HTTPException(status_code=403, detail="Share is not active")
        
        if share_data.get('expires_at'):
            expires_at = datetime.fromisoformat(share_data['expires_at'].replace('Z', ''))
            if datetime.utcnow() > expires_at:
                raise HTTPException(status_code=410, detail="Share has expired")
        
        # Get appropriate service
        if share_data.get('use_personal_drive', False):
            service = get_user_google_service(share_data['sender_email'], share_data.get('drive_id', 'drive_1'))
        else:
            service = get_google_service()
        
        if not service:
            raise HTTPException(status_code=500, detail="Service unavailable")
        
        # Get file metadata and check if it's a folder
        file_id = share_data['file_id']
        file_metadata = service.files().get(fileId=file_id, fields='id,name,mimeType,size').execute()
        
        # Check if it's a folder
        if file_metadata.get('mimeType') == 'application/vnd.google-apps.folder':
            # Handle folder download as ZIP
            import zipfile
            import tempfile
            
            def generate_folder_zip():
                with tempfile.NamedTemporaryFile() as temp_file:
                    with zipfile.ZipFile(temp_file, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                        # Get all files in the folder
                        results = service.files().list(
                            q=f"'{file_id}' in parents and trashed=false",
                            fields="files(id,name,mimeType)"
                        ).execute()
                        
                        for file_item in results.get('files', []):
                            if file_item.get('mimeType') != 'application/vnd.google-apps.folder':
                                try:
                                    # Download file content
                                    request = service.files().get_media(fileId=file_item['id'])
                                    file_io = io.BytesIO()
                                    downloader = MediaIoBaseDownload(file_io, request)
                                    done = False
                                    while not done:
                                        status, done = downloader.next_chunk()
                                    
                                    # Add to ZIP
                                    zip_file.writestr(file_item['name'], file_io.getvalue())
                                except Exception as e:
                                    print(f"Error adding file {file_item['name']} to ZIP: {str(e)}")
                    
                    temp_file.seek(0)
                    while True:
                        chunk = temp_file.read(8192)
                        if not chunk:
                            break
                        yield chunk
            
            folder_name = file_metadata['name']
            import re
            safe_filename = re.sub(r'[<>:"/\\|?*]', '_', folder_name)
            # Remove Unicode characters that can't be encoded in Latin-1
            safe_filename = safe_filename.encode('ascii', 'ignore').decode('ascii')
            if not safe_filename or safe_filename.strip() == '':
                safe_filename = 'folder'
            safe_filename += '.zip'
            
            return StreamingResponse(
                generate_folder_zip(),
                media_type='application/zip',
                headers={
                    'Content-Disposition': f'attachment; filename="{safe_filename}"',
                    'Cache-Control': 'no-cache',
                    'Access-Control-Allow-Origin': '*'
                }
            )
        
        # Handle regular file download
        def generate_stream():
            request = service.files().get_media(fileId=file_id)
            file_io = io.BytesIO()
            downloader = MediaIoBaseDownload(file_io, request, chunksize=1024*1024)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    file_io.seek(0)
                    chunk = file_io.read()
                    if chunk:
                        yield chunk
                    file_io.seek(0)
                    file_io.truncate(0)
        
        filename = file_metadata.get('name', 'file')
        import re
        safe_filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Remove Unicode characters that can't be encoded in Latin-1
        safe_filename = safe_filename.encode('ascii', 'ignore').decode('ascii')
        if not safe_filename or safe_filename.strip() == '':
            file_extension = filename.lower().split('.')[-1] if '.' in filename else ''
            safe_filename = f'file.{file_extension}' if file_extension else 'file'
        
        # Comprehensive MIME type mapping for download
        file_extension = filename.lower().split('.')[-1] if '.' in filename else ''
        mime_types = {
            # Images
            'png': 'image/png', 'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'gif': 'image/gif',
            'bmp': 'image/bmp', 'webp': 'image/webp', 'svg': 'image/svg+xml', 'ico': 'image/x-icon',
            'tiff': 'image/tiff', 'tif': 'image/tiff',
            # Documents
            'pdf': 'application/pdf', 'txt': 'text/plain', 'csv': 'text/csv', 'md': 'text/markdown',
            'json': 'application/json', 'xml': 'application/xml', 'html': 'text/html', 'htm': 'text/html',
            'css': 'text/css', 'js': 'application/javascript', 'jsx': 'text/jsx', 'ts': 'text/typescript',
            'tsx': 'text/tsx', 'py': 'text/x-python', 'java': 'text/x-java-source', 'c': 'text/x-c',
            'cpp': 'text/x-c++', 'h': 'text/x-c', 'hpp': 'text/x-c++', 'php': 'text/x-php',
            'rb': 'text/x-ruby', 'go': 'text/x-go', 'rs': 'text/x-rust', 'kt': 'text/x-kotlin',
            'swift': 'text/x-swift', 'dart': 'text/x-dart', 'sh': 'text/x-shellscript',
            'bat': 'text/x-msdos-batch', 'ps1': 'text/x-powershell', 'sql': 'text/x-sql',
            'yaml': 'text/yaml', 'yml': 'text/yaml', 'toml': 'text/x-toml', 'ini': 'text/plain',
            'cfg': 'text/plain', 'conf': 'text/plain', 'log': 'text/plain',
            # Office documents
            'doc': 'application/msword', 'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'xls': 'application/vnd.ms-excel', 'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'ppt': 'application/vnd.ms-powerpoint', 'pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            # Archives
            'zip': 'application/zip', 'rar': 'application/x-rar-compressed', '7z': 'application/x-7z-compressed',
            'tar': 'application/x-tar', 'gz': 'application/gzip', 'bz2': 'application/x-bzip2',
            'xz': 'application/x-xz',
            # Videos
            'mp4': 'video/mp4', 'avi': 'video/x-msvideo', 'mov': 'video/quicktime', 'wmv': 'video/x-ms-wmv',
            'flv': 'video/x-flv', 'webm': 'video/webm', 'mkv': 'video/x-matroska', 'm4v': 'video/mp4',
            '3gp': 'video/3gpp', 'ogv': 'video/ogg',
            # Audio
            'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'ogg': 'audio/ogg', 'aac': 'audio/aac',
            'flac': 'audio/flac', 'm4a': 'audio/mp4', 'wma': 'audio/x-ms-wma'
        }
        proper_mime_type = mime_types.get(file_extension, 'application/octet-stream')
        
        return StreamingResponse(
            generate_stream(),
            media_type=proper_mime_type,
            headers={
                'Content-Disposition': f'attachment; filename="{safe_filename}"',
                'Cache-Control': 'no-cache',
                'Access-Control-Allow-Origin': '*',
                'Accept-Ranges': 'bytes'
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error downloading shared file: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to download file")

@app.delete("/shared/email/{share_id}")
async def delete_email_share(share_id: str, current_user: str = Depends(get_current_user)):
    """Delete/expire an email share - requires authentication"""
    if not db:
        raise HTTPException(status_code=500, detail="Database not available")
    
    try:
        doc_ref = db.collection('email_shares').document(share_id)
        doc = doc_ref.get()
        
        if not doc.exists:
            raise HTTPException(status_code=404, detail="Share not found")
        
        share_data = doc.to_dict()
        
        # Check if user is authorized (sender or recipient)
        if share_data.get('sender_email') != current_user and share_data.get('recipient_email') != current_user:
            raise HTTPException(status_code=403, detail="Not authorized to delete this share")
        
        # Expire the share instead of deleting
        doc_ref.update({
            'expires_at': datetime.utcnow().isoformat() + 'Z',
            'status': 'expired',
            'deleted_by': current_user,
            'deleted_at': datetime.utcnow().isoformat() + 'Z'
        })
        
        return {"message": "Share deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error deleting email share: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to delete share")

@app.post("/shared/toggle/{share_id}")
async def toggle_email_share(share_id: str, current_user: str = Depends(get_current_user)):
    """Toggle email share status (activate/deactivate) - requires authentication"""
    if not db:
        raise HTTPException(status_code=500, detail="Database not available")
    
    try:
        doc_ref = db.collection('email_shares').document(share_id)
        doc = doc_ref.get()
        
        if not doc.exists:
            raise HTTPException(status_code=404, detail="Share not found")
        
        share_data = doc.to_dict()
        
        # Only sender can toggle
        if share_data.get('sender_email') != current_user:
            raise HTTPException(status_code=403, detail="Not authorized to modify this share")
        
        current_status = share_data.get('status', 'active')
        new_status = 'inactive' if current_status == 'active' else 'active'
        
        # If reactivating, check if not manually expired
        if new_status == 'active' and share_data.get('expires_at'):
            expires_at = datetime.fromisoformat(share_data['expires_at'].replace('Z', ''))
            if datetime.utcnow() > expires_at:
                raise HTTPException(status_code=400, detail="Cannot reactivate expired share")
        
        doc_ref.update({
            'status': new_status,
            'toggled_at': datetime.utcnow().isoformat() + 'Z',
            'toggled_by': current_user
        })
        
        return {"message": f"Share {new_status}"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error toggling email share: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to toggle share")

@app.post("/shared/send")
async def send_share_request(request: ShareRequest, current_user: str = Depends(get_current_user)):
    """Send share request to another user"""
    if not db:
        raise HTTPException(status_code=500, detail="Database not available")
    
    if request.recipient_email == current_user:
        raise HTTPException(status_code=400, detail="Cannot share with yourself")
    
    # Check if recipient exists
    try:
        auth.get_user_by_email(request.recipient_email)
    except:
        raise HTTPException(status_code=404, detail="User not found in NovaCloud")
    
    try:
        share_data = {
            'sender_email': current_user,
            'recipient_email': request.recipient_email,
            'status': 'pending',
            'created_at': datetime.utcnow().isoformat()
        }
        
        db.collection('user_shares').add(share_data)
        return {"message": "Share request sent successfully"}
        
    except Exception as e:
        print(f"Error sending share request: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to send share request")

@app.post("/shared/accept/{share_id}")
async def accept_share_request(share_id: str, current_user: str = Depends(get_current_user)):
    """Accept share request"""
    if not db:
        raise HTTPException(status_code=500, detail="Database not available")
    
    try:
        doc_ref = db.collection('user_shares').document(share_id)
        doc = doc_ref.get()
        
        if not doc.exists:
            raise HTTPException(status_code=404, detail="Share request not found")
        
        share_data = doc.to_dict()
        if share_data.get('recipient_email') != current_user:
            raise HTTPException(status_code=403, detail="Not authorized")
        
        doc_ref.update({
            'status': 'accepted',
            'accepted_at': datetime.utcnow().isoformat()
        })
        
        return {"message": "Share request accepted"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error accepting share: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to accept share request")

@app.post("/shared/reject/{share_id}")
async def reject_share_request(share_id: str, current_user: str = Depends(get_current_user)):
    """Reject share request"""
    if not db:
        raise HTTPException(status_code=500, detail="Database not available")
    
    try:
        doc_ref = db.collection('user_shares').document(share_id)
        doc = doc_ref.get()
        
        if not doc.exists:
            raise HTTPException(status_code=404, detail="Share request not found")
        
        share_data = doc.to_dict()
        if share_data.get('recipient_email') != current_user:
            raise HTTPException(status_code=403, detail="Not authorized")
        
        doc_ref.update({
            'status': 'rejected',
            'rejected_at': datetime.utcnow().isoformat()
        })
        
        return {"message": "Share request rejected"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error rejecting share: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to reject share request")

