import pymongo
import os
from dotenv import load_dotenv

# Charger les variables d'environnement du fichier .env
load_dotenv()

def connect_to_mongodb():
    mongodb_uri = os.getenv("MONGODB_URI")
    client = pymongo.MongoClient(mongodb_uri, tls=True, tlsAllowInvalidCertificates=True)
    db = client['gmail_db']
    return db['emails']
