import pandas as pd
from mongodb_connection import connect_to_mongodb
from snowflake_connection import connect_to_snowflake
from snowflake.connector.pandas_tools import write_pandas

import sys
sys.path.append('..')
from dimension.restaurant_dim import clean_and_extract_info as clean_and_extract_restaurant_info, create_restaurant_dataframe
from dimension.paiement_dim import clean_and_extract_payment_info, create_payment_dataframe
from dimension.date_dim import clean_and_extract_date_info
from fact.commandes_fact import create_commandes_dataframe

def create_body_dataframe(collection, number_of_documents):
    documents = collection.find({"from": "\"Reçu Uber\" <noreply@uber.com>", "subject": {"$regex": "Votre commande"}}).limit(number_of_documents)
    body_list = [doc['body'] for doc in documents]
    df = pd.DataFrame(body_list, columns=['Body'])
    df['Body'] = df['Body'].str.replace('\r\n', ' ', regex=False)
    return df

def main():
    number_of_documents_to_display = 499
    emails_collection = connect_to_mongodb()

    # Extraction et nettoyage des données d'e-mails
    body_df = create_body_dataframe(emails_collection, number_of_documents_to_display)

    # Traitement pour les dimensions
    cleaned_restaurant_df = clean_and_extract_restaurant_info(body_df)
    restaurant_df = create_restaurant_dataframe(cleaned_restaurant_df.to_dict('records'))

    cleaned_payment_df = clean_and_extract_payment_info(body_df)
    payment_df = create_payment_dataframe(cleaned_payment_df.to_dict('records'))

    body_df['DateTime'] = body_df['Body'].str.extract(r'(\d{2}/\d{2}/\d{4} \d{2}:\d{2})')
    print("Extracted DateTime:\n", body_df['DateTime'].head())  # Debug: Check extracted DateTime

    date_df = clean_and_extract_date_info(body_df)
    print("DataFrame after Date Extraction:\n", date_df.head()) 
    date_df.reset_index(drop=True, inplace=True)
    print(date_df.columns)

    # Création de la table de faits Commandes
    commandes_df = create_commandes_dataframe(body_df, restaurant_df, payment_df, date_df)

    # Chargement dans Snowflake
    conn = connect_to_snowflake()

    try:
        write_pandas(conn, restaurant_df, 'RESTAURANT_DIM')
        print("Data loaded successfully into 'RESTAURANT_DIM'")
    except Exception as e:
        print(f"Error loading data into 'RESTAURANT_DIM': {e}")

    try:
        write_pandas(conn, payment_df, 'PAIEMENT_DIM')
        print("Data loaded successfully into 'PAIEMENT_DIM'")
    except Exception as e:
        print(f"Error loading data into 'PAIEMENT_DIM': {e}")

    try:
        write_pandas(conn, date_df, 'DATE_DIM')
        print("Data loaded successfully into 'DATE_DIM'")
    except Exception as e:
        print(f"Error loading data into 'DATE_DIM': {e}")

    try:
        write_pandas(conn, commandes_df, 'COMMANDES_FACT')
        print("Data loaded successfully into 'COMMANDES_FACT'")
    except Exception as e:
        print(f"Error loading data into 'COMMANDES_FACT': {e}")

    conn.close()

    # Affichage pour vérification
    print(restaurant_df)
    print(payment_df)
    print(date_df)
    print(commandes_df)

if __name__ == '__main__':
    main()
