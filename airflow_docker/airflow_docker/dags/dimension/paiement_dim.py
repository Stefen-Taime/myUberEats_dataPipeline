import pandas as pd
import hashlib
import re

def generate_id(value):
    """ Génère un ID unique basé sur la valeur donnée. """
    return hashlib.md5(value.encode()).hexdigest()

def clean_and_extract_payment_info(df):
    df['Body'] = df['Body'].str.replace(r'\r\n', ' ')  # Nettoyage des retours à la ligne
    df['TypePaiement'] = df['Body'].str.extract(r'Paiements\s+(\w+)')  # Extraction du type de paiement

    # Mise à jour de l'expression régulière pour l'extraction du montant
    df['Montant'] = df['Body'].str.extract(r'Total\s*([\d,\.]+)\s*\$CA')
    df['Montant'] = df['Montant'].str.replace(',', '.').astype(float)  # Conversion en float et gestion des virgules et points

    # Gestion des valeurs manquantes pour le montant
    df['Montant'].fillna(0.0, inplace=True)

    # Débogage pour vérifier les montants extraits
    print(df[['Body', 'Montant']])

    return df


def create_payment_dataframe(emails):
    payments = []
    for index, email in enumerate(emails):
        try:
            type_paiement = str(email.get('TypePaiement', "Type inconnu"))
            montant = email.get('Montant', 0.0)  # Use 0.0 as default for missing amounts

            # Convert montant to string and ensure type_paiement is also a string
            payment_id = generate_id(type_paiement + str(montant))

            payment_info = {
                "ID_Paiement": payment_id,
                "TypePaiement": type_paiement,
                "Montant": montant,
            }
            payments.append(payment_info)
        except Exception as e:
            # Log the error and problematic data
            print(f"Error processing row {index}: {e}")
            print(f"Data: TypePaiement={type_paiement}, Montant={montant}")

    df = pd.DataFrame(payments)
    return df
