from fastapi import FastAPI, HTTPException, Depends, UploadFile, File, Form, WebSocket
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, StreamingResponse
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

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
from parallel_api import parallel_processor, async_parallel

load_dotenv()

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
        
        if not email:
            raise HTTPException(status_code=401, detail="Email not found in token")
        
        return email
    except Exception as e:
        print(f"Auth Error: {str(e)}")
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")



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

def save_user_to_firestore(user_email: str, user_data: dict):
    """Save user data to Firestore"""
    if not db:
        print(f"Firestore not available, cannot save user {user_email}")
        return False
    try:
        doc_ref = db.collection('users').document(user_email)
        doc_ref.set(user_data, merge=True)
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
        return doc.exists
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

def get_user_folder(service, user_name: str):
    folder_name = user_name.replace(' ', '_')
    
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

def create_user_folder(user_name: str):
    service = get_google_service()
    if not service:
        return None
    try:
        return get_user_folder(service, user_name)
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
                    "storage_limit": 16106127360,
                    "limit_display": "15 GB",
                    "storage_percentage": (total_size / 16106127360 * 100) if total_size > 0 else 0
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
@app.get("/")
async def root():
    return {"message": "Novacloud API - Made in India 🇮🇳"}

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
    return {"exists": check_user_exists_in_firestore(email)}

@app.post("/register-user")
async def register_user(user_data: UserCreate):
    """Register a new user in the database"""
    if check_user_exists_in_firestore(user_data.email):
        raise HTTPException(status_code=400, detail="User already exists")
    
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
    
    if not save_user_to_firestore(user_data.email, new_user):
        raise HTTPException(status_code=500, detail="Failed to register user")
    
    return {"message": "User registered successfully"}



@app.get("/setup-drive")
async def setup_drive():
    try:
        flow = Flow.from_client_secrets_file(
            'credentials.json',
            scopes=['https://www.googleapis.com/auth/drive.file']
        )
        flow.redirect_uri = GOOGLE_REDIRECT_URI
        
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
                'https://www.googleapis.com/auth/userinfo.email',
                'https://www.googleapis.com/auth/userinfo.profile',
                'openid'
            ]
        )
        flow.redirect_uri = GOOGLE_REDIRECT_URI
        
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
async def oauth2callback(code: str, state: Optional[str] = None):
    frontend_url = os.getenv('FRONTEND_URL', 'http://localhost:3000')
    
    try:
        flow = Flow.from_client_secrets_file(
            'credentials.json',
            scopes=[
                'https://www.googleapis.com/auth/drive.file',
                'https://www.googleapis.com/auth/userinfo.email',
                'https://www.googleapis.com/auth/userinfo.profile',
                'openid'
            ]
        )
        flow.redirect_uri = GOOGLE_REDIRECT_URI
        
        flow.fetch_token(code=code)
        credentials = flow.credentials
        
        # Ensure we have at least the drive.file scope
        if 'https://www.googleapis.com/auth/drive.file' not in credentials.scopes:
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
                    
                # Redirect to frontend with success message
                return RedirectResponse(
                    url=f"{frontend_url}/dashboard?drive_connected=true&drive_id={drive_id}",
                    status_code=302
                )
            else:
                # Legacy format support
                user_email = state.replace('user:', '').split(':')[0]
                print(f"Legacy format - User: {user_email}")
                
                user_tokens = get_user_drive_tokens_from_firestore(user_email)
                token_data = json.loads(credentials.to_json())
                user_tokens['drive_1'] = token_data
                save_user_drive_tokens_to_firestore(user_email, user_tokens)
                    
                print(f"Legacy format - Successfully saved tokens for {user_email} - drive_1")
                    
                return RedirectResponse(
                    url=f"{frontend_url}/dashboard?drive_connected=true&drive_id=drive_1",
                    status_code=302
                )
        else:
            # Save shared tokens for admin setup
            save_json_file('shared_tokens.json', json.loads(credentials.to_json()))
            return {"message": "Shared Google Drive authorized successfully!"}
            
    except Exception as e:
        print(f"OAuth callback error: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        
        if state and state.startswith('user:'):
            return RedirectResponse(
                url=f"{frontend_url}/dashboard?error=auth_failed&message={str(e)}",
                status_code=302
            )
        return {"error": f"Authorization failed: {str(e)}"}

@app.post("/create-folder")
async def create_folder(
    folder_name: str = Form(...),
    parent_folder_id: Optional[str] = Form(None),
    use_personal_drive: bool = Form(False),
    drive_id: str = Form("drive_1"),
    overwrite: bool = Form(False),
    current_user: str = Depends(get_current_user)
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
            user_folder_id = get_user_folder(service, user_data['name'])
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
        
        file_metadata = {
            'name': file.filename,
            'parents': [target_folder_id]
        }
        
        media = MediaIoBaseUpload(
            io.BytesIO(file_content),
            mimetype=file.content_type or 'application/octet-stream',
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
    current_user: str = Depends(get_current_user)
):
    if use_personal_drive:
        service = get_user_google_service(current_user, drive_id)
        if not service:
            return []
        folder_query = "'root' in parents and trashed=false"
    else:
        service = get_google_service()
        if not service:
            return []
        
        user_data = get_user_from_firestore(current_user)
        if not user_data:
            return []
        
        user_folder_id = user_data.get('folder_id')
        if not user_folder_id:
            return []
        folder_query = f"'{user_folder_id}' in parents and trashed=false"
    
    try:
        results = service.files().list(
            q=folder_query,
            fields="files(id,name,size,mimeType,createdTime,webViewLink,webContentLink)"
        ).execute()
        files = results.get('files', [])
        
        # Process files and calculate folder sizes
        processed_files = []
        for file in files:
            if file.get('mimeType') == 'application/vnd.google-apps.folder':
                # Calculate folder size
                folder_files = service.files().list(
                    q=f"'{file['id']}' in parents and trashed=false",
                    fields="files(size)"
                ).execute().get('files', [])
                total_size = sum(int(f.get('size', 0)) for f in folder_files if f.get('size'))
                file['size'] = total_size
            processed_files.append(FileInfo(**file))
        
        return processed_files
    except:
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

def get_optional_current_user(credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer(auto_error=False))):
    if not credentials:
        return None
    try:
        decoded_token = auth.verify_id_token(credentials.credentials)
        return decoded_token.get('email')
    except:
        return None

@app.get("/preview/{file_id}")
async def preview_file(
    file_id: str, 
    use_personal_drive: bool = False,
    drive_id: str = "drive_1",
    current_user: Optional[str] = Depends(get_optional_current_user)
):
    if use_personal_drive and current_user:
        service = get_user_google_service(current_user, drive_id)
    else:
        service = get_google_service()
    
    if not service:
        raise HTTPException(status_code=500, detail="Google Drive not configured")
    
    try:
        file_metadata = service.files().get(fileId=file_id).execute()
        request = service.files().get_media(fileId=file_id)
        
        file_io = io.BytesIO()
        downloader = MediaIoBaseDownload(file_io, request)
        
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        
        file_io.seek(0)
        
        # For CSV files, return as plain text with inline disposition
        mime_type = file_metadata.get('mimeType', 'application/octet-stream')
        file_name = file_metadata.get('name', '').lower()
        headers = {}
        
        if mime_type == 'text/csv' or file_name.endswith('.csv'):
            mime_type = 'text/plain'
            headers['Content-Disposition'] = 'inline'
        
        return StreamingResponse(
            io.BytesIO(file_io.read()),
            media_type=mime_type,
            headers=headers
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Preview failed: {str(e)}")

@app.get("/download/{file_id}")
async def download_file(
    file_id: str, 
    use_personal_drive: bool = False,
    drive_id: str = "drive_1",
    current_user: str = Depends(get_current_user)
):
    if use_personal_drive:
        service = get_user_google_service(current_user, drive_id)
    else:
        service = get_google_service()
    
    file_metadata = service.files().get(fileId=file_id).execute()
    request = service.files().get_media(fileId=file_id)
    
    file_io = io.BytesIO()
    downloader = MediaIoBaseDownload(file_io, request)
    
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    
    file_io.seek(0)
    
    filename = file_metadata['name']
    return StreamingResponse(
        io.BytesIO(file_io.read()),
        media_type='application/octet-stream',
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

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
        # Update user storage in real-time
        update_user_storage(current_user)
        return {"message": "File deleted successfully"}
    except Exception as e:
        if "File not found" in str(e) or "404" in str(e):
            return {"message": "File already deleted or not found"}
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")













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
        
        # Real storage calculation: 15GB per registered user
        total_storage = total_registered * 15
        
        return {
            "active_users": f"{active_users}" if active_users > 0 else "0",
            "uptime": f"{uptime}%",
            "free_storage": "15GB",
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
            "free_storage": "15GB",
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
    
    storage_limit = 16106127360  # 15GB
    storage_percentage = (storage_used / storage_limit * 100) if storage_used > 0 else 0
    
    return {
        "storage_used": storage_used,
        "storage_display": format_size(storage_used),
        "storage_limit": storage_limit,
        "limit_display": "15 GB",
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
    if not check_user_exists_in_firestore(current_user):
        raise HTTPException(status_code=404, detail="User not found")
    
    # Get user data and ensure folder exists
    user_data = get_user_from_firestore(current_user)
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
    
    storage_limit = 16106127360  # 15GB
    
    return {
        "storage_used": total_size,
        "storage_display": format_size(total_size),
        "storage_limit": storage_limit,
        "limit_display": "15 GB",
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
        storage_limit = 16106127360  # 15GB per user
        
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
    
    old_name = user_data.get('name', '')
    
    # Update Google Drive folder name if it exists
    service = get_google_service()
    if service and user_data.get('folder_id'):
        try:
            old_folder_name = old_name.replace(' ', '_')
            new_folder_name = name.replace(' ', '_')
            
            if old_folder_name != new_folder_name:
                service.files().update(
                    fileId=user_data['folder_id'],
                    body={'name': new_folder_name}
                ).execute()
                print(f"Renamed folder from {old_folder_name} to {new_folder_name}")
        except Exception as e:
            print(f"Failed to rename Google Drive folder: {str(e)}")
    
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
{os.getenv('FRONTEND_URL', 'http://localhost:3000')}/verify-email-change?token={verification_token}

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
        
        # Delete from Firestore collections
        if db:
            # Delete user data
            try:
                db.collection('users').document(current_user).delete()
                print(f"Deleted user data from Firestore")
            except Exception as e:
                print(f"Error deleting user data: {str(e)}")
            
            # Delete 2FA data
            try:
                db.collection('user_2fa').document(current_user).delete()
                print(f"Deleted 2FA data")
            except Exception as e:
                print(f"Error deleting 2FA data: {str(e)}")
            
            # Delete drive tokens
            try:
                db.collection('user_drive_tokens').document(current_user).delete()
                print(f"Deleted drive tokens")
            except Exception as e:
                print(f"Error deleting drive tokens: {str(e)}")
        
        # Delete Firebase user
        auth.delete_user(firebase_user.uid)
        print(f"Deleted Firebase user: {firebase_user.uid}")
        
        return {"message": "Account deleted successfully"}
        
    except Exception as e:
        print(f"Account deletion error: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to delete account")