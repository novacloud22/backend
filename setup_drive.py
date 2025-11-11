from google_auth_oauthlib.flow import Flow
import json

# Set up the flow
flow = Flow.from_client_secrets_file(
    'credentials.json',
    scopes=['https://www.googleapis.com/auth/drive.file']
)

flow.redirect_uri = 'http://localhost:8000/oauth2callback'

# Get authorization URL
auth_url, _ = flow.authorization_url(
    access_type='offline',
    include_granted_scopes='true',
    prompt='consent'  # Force consent to get refresh_token
)

print("Go to this URL to authorize the shared Google Drive:")
print(auth_url)
print("\nAfter authorization, paste the full callback URL here:")
callback_url = input("Callback URL: ")

# Extract authorization code from callback URL
flow.fetch_token(authorization_response=callback_url)

# Save the credentials
creds = flow.credentials
token_data = {
    'token': creds.token,
    'refresh_token': creds.refresh_token,
    'token_uri': creds.token_uri,
    'client_id': creds.client_id,
    'client_secret': creds.client_secret,
    'scopes': creds.scopes
}

with open('token.json', 'w') as token_file:
    json.dump(token_data, token_file)

print("\nTokens saved successfully to token.json!")