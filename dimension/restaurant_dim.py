import pandas as pd
import hashlib
import re

def generate_id(value):
    """ Génère un ID unique basé sur la valeur donnée. """
    return hashlib.md5(value.encode()).hexdigest()

def clean_and_extract_info(df):
    df['Body'] = df['Body'].str.replace(r'\r\n', ' ')  # Nettoyage des retours à la ligne
    df['Total'] = df['Body'].str.extract(r'Total\s*([\d,]+(?:\.\d+)? \$CA)')  # Ajustement pour extraire le montant total
    df['Restaurant'] = df['Body'].str.extract(r'Merci d\'avoir passé une commande,.*?facture pour (.*?)\.')  # Extraction du nom du restaurant
    return df

def create_restaurant_dataframe(emails):
    restaurants = []
    for email in emails:
        restaurant_name = email.get('Restaurant', "Nom inconnu")
        if isinstance(restaurant_name, float):
            restaurant_name = "Nom inconnu"

        restaurant_id = generate_id(restaurant_name)

        restaurant_info = {
            "ID_Restaurant": restaurant_id,
            "NomRestaurant": restaurant_name,
            # Autres champs si nécessaire
        }
        restaurants.append(restaurant_info)
    df = pd.DataFrame(restaurants)
    return df
