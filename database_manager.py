import os
import json
import time
from typing import Dict, Optional, Any
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import Client

class MultiFirestoreManager:
    def __init__(self):
        self.auth_app = None
        self.db = None
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize single Firestore database"""
        firebase_creds_json = os.getenv('FIREBASE_SERVICE_ACCOUNT_JSON')
        
        if firebase_creds_json:
            try:
                cred = credentials.Certificate(json.loads(firebase_creds_json))
                self.auth_app = firebase_admin.initialize_app(cred)
                self.db = firestore.client()
                print("✓ Firebase initialized")
            except Exception as e:
                print(f"Firebase init failed: {e}")
        else:
            print("✗ No Firebase credentials")
    
    def get_database_for_user(self, user_id: str) -> Optional[Client]:
        return self.db
    
    def get_auth_app(self):
        """Get the main Firebase Auth app"""
        return self.auth_app
    
    def _retry_operation(self, operation, *args, **kwargs):
        for attempt in range(3):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                if attempt == 2:
                    raise e
                time.sleep(0.5)
    
    def save_user_data(self, user_email: str, data: dict) -> bool:
        if not self.db:
            return False
        try:
            self._retry_operation(lambda: self.db.collection('users').document(user_email).set(data, merge=True))
            return True
        except Exception as e:
            print(f"Save user failed: {e}")
            return False
    
    def get_user_data(self, user_email: str) -> Optional[dict]:
        if not self.db:
            return None
        try:
            doc = self._retry_operation(lambda: self.db.collection('users').document(user_email).get())
            return doc.to_dict() if doc.exists else None
        except Exception as e:
            print(f"Get user failed: {e}")
            return None
    
    def update_user_data(self, user_email: str, updates: dict) -> bool:
        if not self.db:
            return False
        try:
            self._retry_operation(lambda: self.db.collection('users').document(user_email).update(updates))
            return True
        except Exception as e:
            print(f"Update user failed: {e}")
            return False
    
    def delete_user_data(self, user_email: str) -> bool:
        if not self.db:
            return False
        try:
            self._retry_operation(lambda: self.db.collection('users').document(user_email).delete())
            return True
        except Exception as e:
            print(f"Delete user failed: {e}")
            return False
    
    def save_user_tokens(self, user_email: str, tokens: dict) -> bool:
        if not self.db:
            return False
        try:
            self._retry_operation(lambda: self.db.collection('user_drive_tokens').document(user_email).set(tokens))
            return True
        except Exception as e:
            print(f"Save tokens failed: {e}")
            return False
    
    def get_user_tokens(self, user_email: str) -> dict:
        if not self.db:
            return {}
        try:
            doc = self._retry_operation(lambda: self.db.collection('user_drive_tokens').document(user_email).get())
            return doc.to_dict() if doc.exists else {}
        except Exception as e:
            print(f"Get tokens failed: {e}")
            return {}

# Global instance
db_manager = MultiFirestoreManager()
db = db_manager.db
