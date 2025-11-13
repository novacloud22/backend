import os
import json
import hashlib
from typing import Dict, Optional, Any
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1 import Client

class MultiFirestoreManager:
    def __init__(self):
        self.auth_app = None
        self.firestore_clients: Dict[str, Client] = {}
        self.database_count = 0
        self._initialize_databases()
    
    def _initialize_databases(self):
        """Initialize all Firestore databases from service account files"""
        # Initialize main auth project using FIREBASE_SERVICE_ACCOUNT_JSON
        firebase_creds_json = os.getenv('FIREBASE_SERVICE_ACCOUNT_JSON')
        if firebase_creds_json:
            try:
                auth_cred = credentials.Certificate(json.loads(firebase_creds_json))
                self.auth_app = firebase_admin.initialize_app(auth_cred, name='auth')
                
                # Use main project as database too
                main_app = firebase_admin.initialize_app(auth_cred, name='main_db')
                self.firestore_clients['main'] = firestore.client(main_app)
                self.database_count += 1
                print(f"Initialized main project as auth + database: main")
            except Exception as e:
                print(f"Failed to initialize main project: {e}")
        else:
            # Fallback to file
            auth_cred_path = 'firebase-service-account.json'
            if os.path.exists(auth_cred_path):
                auth_cred = credentials.Certificate(auth_cred_path)
                self.auth_app = firebase_admin.initialize_app(auth_cred, name='auth')
                
                # Use main project as database too
                main_app = firebase_admin.initialize_app(auth_cred, name='main_db')
                self.firestore_clients['main'] = firestore.client(main_app)
                self.database_count += 1
                print(f"Initialized main project as auth + database: main")
        
        # Initialize additional databases from environment variables
        for i in range(1, 10):
            db_cred_json = os.getenv(f'DB{i}_CREDENTIALS')
            if db_cred_json:
                try:
                    db_name = f'db{i}'
                    cred = credentials.Certificate(json.loads(db_cred_json))
                    app = firebase_admin.initialize_app(cred, name=f'db_{db_name}')
                    self.firestore_clients[db_name] = firestore.client(app)
                    self.database_count += 1
                    print(f"Initialized Firestore database: {db_name}")
                except Exception as e:
                    print(f"Failed to initialize DB{i}: {e}")
        
        # Fallback: Initialize from local files if no env vars
        if self.database_count <= 1:  # Only main database initialized
            db_dir = 'firestore_credentials'
            if os.path.exists(db_dir):
                for filename in os.listdir(db_dir):
                    if filename.endswith('.json'):
                        db_name = filename.replace('.json', '')
                        cred_path = os.path.join(db_dir, filename)
                        try:
                            cred = credentials.Certificate(cred_path)
                            app = firebase_admin.initialize_app(cred, name=f'db_{db_name}')
                            self.firestore_clients[db_name] = firestore.client(app)
                            self.database_count += 1
                            print(f"Initialized Firestore database: {db_name}")
                        except Exception as e:
                            print(f"Failed to initialize {db_name}: {e}")
    
    def get_database_for_user(self, user_id: str) -> Optional[Client]:
        """Select database based on user ID hash for load balancing"""
        if not self.firestore_clients:
            return None
        
        # Use hash of user_id to consistently assign users to databases
        hash_value = int(hashlib.md5(user_id.encode()).hexdigest(), 16)
        db_index = hash_value % len(self.firestore_clients)
        db_name = list(self.firestore_clients.keys())[db_index]
        return self.firestore_clients[db_name]
    
    def get_auth_app(self):
        """Get the main Firebase Auth app"""
        return self.auth_app
    
    def save_user_data(self, user_email: str, data: dict) -> bool:
        """Save user data to assigned Firestore database"""
        db = self.get_database_for_user(user_email)
        if not db:
            return False
        try:
            doc_ref = db.collection('users').document(user_email)
            doc_ref.set(data, merge=True)
            return True
        except Exception as e:
            print(f"Error saving user data: {e}")
            return False
    
    def get_user_data(self, user_email: str) -> Optional[dict]:
        """Get user data from assigned Firestore database"""
        db = self.get_database_for_user(user_email)
        if not db:
            return None
        try:
            doc_ref = db.collection('users').document(user_email)
            doc = doc_ref.get()
            return doc.to_dict() if doc.exists else None
        except Exception as e:
            print(f"Error getting user data: {e}")
            return None
    
    def update_user_data(self, user_email: str, updates: dict) -> bool:
        """Update user data in assigned Firestore database"""
        db = self.get_database_for_user(user_email)
        if not db:
            return False
        try:
            doc_ref = db.collection('users').document(user_email)
            doc_ref.update(updates)
            return True
        except Exception as e:
            print(f"Error updating user data: {e}")
            return False
    
    def delete_user_data(self, user_email: str) -> bool:
        """Delete user data from assigned Firestore database"""
        db = self.get_database_for_user(user_email)
        if not db:
            return False
        try:
            doc_ref = db.collection('users').document(user_email)
            doc_ref.delete()
            return True
        except Exception as e:
            print(f"Error deleting user data: {e}")
            return False
    
    def save_user_tokens(self, user_email: str, tokens: dict) -> bool:
        """Save user drive tokens to assigned Firestore database"""
        db = self.get_database_for_user(user_email)
        if not db:
            return False
        try:
            doc_ref = db.collection('user_drive_tokens').document(user_email)
            doc_ref.set(tokens)
            return True
        except Exception as e:
            print(f"Error saving user tokens: {e}")
            return False
    
    def get_user_tokens(self, user_email: str) -> dict:
        """Get user drive tokens from assigned Firestore database"""
        db = self.get_database_for_user(user_email)
        if not db:
            return {}
        try:
            doc_ref = db.collection('user_drive_tokens').document(user_email)
            doc = doc_ref.get()
            return doc.to_dict() if doc.exists else {}
        except Exception as e:
            print(f"Error getting user tokens: {e}")
            return {}

# Global instance
db_manager = MultiFirestoreManager()
