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
        self.db2 = None
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize dual Firestore databases"""
        # Primary database
        firebase_creds_json = os.getenv('FIREBASE_SERVICE_ACCOUNT_JSON')
        if firebase_creds_json:
            try:
                cred = credentials.Certificate(json.loads(firebase_creds_json))
                self.auth_app = firebase_admin.initialize_app(cred)
                self.db = firestore.client()
                print("✓ Primary Firebase initialized")
            except Exception as e:
                print(f"Primary Firebase init failed: {e}")
        
        # Secondary database
        firebase_creds_json2 = os.getenv('FIREBASE_SERVICE_ACCOUNT_JSON2')
        if firebase_creds_json2:
            try:
                cred2 = credentials.Certificate(json.loads(firebase_creds_json2))
                app2 = firebase_admin.initialize_app(cred2, name='secondary')
                self.db2 = firestore.client(app2)
                print("✓ Secondary Firebase initialized")
            except Exception as e:
                print(f"Secondary Firebase init failed: {e}")
        
        if not self.db and not self.db2:
            print("✗ No Firebase credentials")
    
    def get_database_for_user(self, user_id: str) -> Optional[Client]:
        return self.db
    
    def get_auth_app(self):
        """Get the main Firebase Auth app"""
        return self.auth_app
    
    def _get_available_db(self):
        """Get available database, prefer primary"""
        return self.db if self.db else self.db2
    
    def _retry_operation_with_fallback(self, operation, *args, **kwargs):
        """Try operation on primary, fallback to secondary if quota exceeded"""
        # Try primary database first
        if self.db:
            try:
                return operation(self.db, *args, **kwargs)
            except Exception as e:
                if "quota" in str(e).lower() or "429" in str(e):
                    print(f"Primary DB quota exceeded, switching to secondary")
                else:
                    # For non-quota errors, retry on same DB
                    for attempt in range(2):
                        try:
                            time.sleep(0.5)
                            return operation(self.db, *args, **kwargs)
                        except Exception:
                            if attempt == 1:
                                break
        
        # Try secondary database
        if self.db2:
            try:
                return operation(self.db2, *args, **kwargs)
            except Exception as e:
                print(f"Secondary DB also failed: {e}")
                raise e
        
        raise Exception("No available database")
    
    def save_user_data(self, user_email: str, data: dict) -> bool:
        if not self.db and not self.db2:
            return False
        try:
            self._retry_operation_with_fallback(lambda db: db.collection('users').document(user_email).set(data, merge=True))
            return True
        except Exception as e:
            print(f"Save user failed: {e}")
            return False
    
    def get_user_data(self, user_email: str) -> Optional[dict]:
        if not self.db and not self.db2:
            return None
        try:
            doc = self._retry_operation_with_fallback(lambda db: db.collection('users').document(user_email).get())
            return doc.to_dict() if doc.exists else None
        except Exception as e:
            print(f"Get user failed: {e}")
            return None
    
    def update_user_data(self, user_email: str, updates: dict) -> bool:
        if not self.db and not self.db2:
            return False
        try:
            self._retry_operation_with_fallback(lambda db: db.collection('users').document(user_email).update(updates))
            return True
        except Exception as e:
            print(f"Update user failed: {e}")
            return False
    
    def delete_user_data(self, user_email: str) -> bool:
        if not self.db and not self.db2:
            return False
        try:
            self._retry_operation_with_fallback(lambda db: db.collection('users').document(user_email).delete())
            return True
        except Exception as e:
            print(f"Delete user failed: {e}")
            return False
    
    def save_user_tokens(self, user_email: str, tokens: dict) -> bool:
        if not self.db and not self.db2:
            return False
        try:
            self._retry_operation_with_fallback(lambda db: db.collection('user_drive_tokens').document(user_email).set(tokens))
            return True
        except Exception as e:
            print(f"Save tokens failed: {e}")
            return False
    
    def get_user_tokens(self, user_email: str) -> dict:
        if not self.db and not self.db2:
            return {}
        try:
            doc = self._retry_operation_with_fallback(lambda db: db.collection('user_drive_tokens').document(user_email).get())
            return doc.to_dict() if doc.exists else {}
        except Exception as e:
            print(f"Get tokens failed: {e}")
            return {}

# Global instance
db_manager = MultiFirestoreManager()
db = db_manager._get_available_db()
