import pandas as pd
import re
import hashlib
import sys
sys.path.append('..')
from dimension.restaurant_dim import clean_and_extract_info as clean_and_extract_restaurant_info, create_restaurant_dataframe
from dimension.paiement_dim import clean_and_extract_payment_info, create_payment_dataframe
from dimension.date_dim import clean_and_extract_date_info


def generate_commande_id(email_body):
    return hashlib.md5(email_body.encode()).hexdigest()


def create_body_dataframe(collection, number_of_documents):
    documents = collection.find({"from": "\"Reçu Uber\" <noreply@uber.com>", "subject": {"$regex": "Votre commande"}}).limit(number_of_documents)
    body_list = [doc['body'] for doc in documents]
    df = pd.DataFrame(body_list, columns=['Body'])
    df['DateTime'] = df['Body'].str.extract(r'(\d{2}/\d{2}/\d{4} \d{2}:\d{2})')  # Regex pour extraire la date et l'heure
    df['Body'] = df['Body'].str.replace('\r\n', ' ', regex=False)
    return df


def create_commandes_dataframe(body_df, restaurant_df, payment_df, date_df):
    commandes = []
    for index, row in body_df.iterrows():
        commande_id = generate_commande_id(row['Body'])
        body = row['Body']
        # Extraction des informations du restaurant, du paiement et du montant total
        restaurant_name = re.search(r"facture pour (.*?)\.", body).group(1) if re.search(r"facture pour (.*?)\.", body) else "Nom inconnu"
        type_paiement = re.search(r"Paiements\s+(\w+)", body).group(1) if re.search(r"Paiements\s+(\w+)", body) else "Type inconnu"
        montant_total = re.search(r"Total\s*([\d,\.]+)\s*\$CA", body)
        montant_total = montant_total.group(1) if montant_total else "0.0"
        montant_total = float(montant_total.replace(',', '.'))

        # Extraction de la date et de l'heure
        # Conversion de Timestamp en chaîne de caractères
        datetime_info = row['DateTime'].strftime('%d/%m/%Y %H:%M')
        jour, mois, annee, heure, minute = map(int, re.match(r"(\d{2})/(\d{2})/(\d{4}) (\d{2}):(\d{2})", datetime_info).groups())


        # Trouver les IDs correspondants
        restaurant_id = restaurant_df[restaurant_df['NomRestaurant'] == restaurant_name]['ID_Restaurant'].iloc[0] if not restaurant_df[restaurant_df['NomRestaurant'] == restaurant_name].empty else "ID inconnu"
        paiement_id = payment_df[payment_df['TypePaiement'] == type_paiement]['ID_Paiement'].iloc[0] if not payment_df[payment_df['TypePaiement'] == type_paiement].empty else "ID inconnu"
        matched_date = date_df[
            (date_df['Jour'] == jour) &
            (date_df['Mois'] == mois) &
            (date_df['Annee'] == annee) &
            (date_df['Heure'] == heure) &
            (date_df['Minute'] == minute)
        ]
        date_heure_id = matched_date['DateHeure'].iloc[0] if not matched_date.empty else "ID inconnu"

        # Ajouter les informations à la liste des commandes
        commande_info = {
            "ID_Commande": commande_id,
            "ID_Restaurant": restaurant_id,
            "ID_Paiement": paiement_id,
            "ID_DateHeure": date_heure_id,
            "MontantTotal": montant_total,
        }
        commandes.append(commande_info)

    return pd.DataFrame(commandes)
