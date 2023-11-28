import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from base64 import urlsafe_b64decode
from bs4 import BeautifulSoup
from base64 import urlsafe_b64decode
import pymongo
import os
from dotenv import load_dotenv

# Charger les variables d'environnement du fichier .env
load_dotenv()



SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def get_gmail_service():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=8080)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return build('gmail', 'v1', credentials=creds)

def search_messages(service, query):
    result = service.users().messages().list(userId='me', q=query).execute()
    messages = []
    if 'messages' in result:
        messages.extend(result['messages'])
    while 'nextPageToken' in result:
        page_token = result['nextPageToken']
        result = service.users().messages().list(userId='me', q=query, pageToken=page_token).execute()
        if 'messages' in result:
            messages.extend(result['messages'])
    return messages

def get_message_details(service, message_id):
    message = service.users().messages().get(userId='me', id=message_id, format='full').execute()
    payload = message.get('payload')
    headers = payload.get('headers')

    subject = next((header['value'] for header in headers if header['name'].lower() == 'subject'), "No Subject")
    sender = next((header['value'] for header in headers if header['name'].lower() == 'from'), "No Sender")

    body_text = ""
    if 'parts' in payload:
        parts = payload.get('parts')
        body = next((part['body']['data'] for part in parts if part['mimeType'] == 'text/plain'), None)
        if body:
            body_text = urlsafe_b64decode(body).decode('utf-8')
    else:
        body = payload.get('body').get('data')
        if body:
            body_text = urlsafe_b64decode(body).decode('utf-8')

    # Utilisation de BeautifulSoup pour extraire le texte du contenu HTML
    soup = BeautifulSoup(body_text, 'html.parser')
    text = soup.get_text()

    return subject, sender, text

def send_emails_to_mongodb(service):
    mongodb_uri = os.getenv("MONGODB_URI")
    client = pymongo.MongoClient(mongodb_uri, tls=True, tlsAllowInvalidCertificates=True)

    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
        return

    db = client['gmail_db']
    collection = db['emails']

    messages = search_messages(service, 'from:Reçu Uber noreply@uber.com')
    print(f"Found {len(messages)} messages matching the query.")

    for message in messages[:499]:  # Par exemple, traiter les 500 premiers messages
        subject, sender, body_text = get_message_details(service, message['id'])
        email_data = {
            'message_id': message['id'],
            'from': sender,
            'subject': subject,
            'body': body_text
        }
        collection.insert_one(email_data)
        print(f"Inserted message {message['id']} into MongoDB.")


if __name__ == '__main__':
    service = get_gmail_service()
    send_emails_to_mongodb(service)

    query = "from:Reçu Uber <noreply@uber.com>"  
    messages = search_messages(service, query)
    print(f"Found {len(messages)} messages matching the query.")

    for message in messages[:499]:
        subject, sender, body_text = get_message_details(service, message['id'])
        print(f"Message ID: {message['id']}")
        print(f"From: {sender}")
        print(f"Subject: {subject}")
        print(f"Body: {body_text[:200]}...")
        print("-" * 499)
