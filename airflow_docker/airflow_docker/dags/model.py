import pandas as pd
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from dimension.date_dim import clean_and_extract_date_info
from dimension.paiement_dim import clean_and_extract_payment_info, create_payment_dataframe
from dimension.restaurant_dim import clean_and_extract_info as clean_and_extract_restaurant_info, create_restaurant_dataframe
from fact.commandes_fact import create_commandes_dataframe

def create_body_dataframe(collection, number_of_documents):
    query = {"from": "\"Reçu Uber\" <noreply@uber.com>", "subject": {"$regex": "Votre commande"}}
    documents = collection.find(query).limit(number_of_documents)
    body_list = [doc['body'] for doc in documents]
    return pd.DataFrame(body_list, columns=['Body']).replace({'\r\n': ' '}, regex=True)

def extract_and_transform_data(body_df):
    body_df['DateTime'] = body_df['Body'].str.extract(r'(\d{2}/\d{2}/\d{4} \d{2}:\d{2})')
    date_df = clean_and_extract_date_info(body_df)

    cleaned_payment_df = clean_and_extract_payment_info(body_df)
    payment_df = create_payment_dataframe(cleaned_payment_df.to_dict('records'))

    cleaned_restaurant_df = clean_and_extract_restaurant_info(body_df)
    restaurant_df = create_restaurant_dataframe(cleaned_restaurant_df.to_dict('records'))

    commandes_df = create_commandes_dataframe(body_df, restaurant_df, payment_df, date_df)
    return date_df, payment_df, restaurant_df, commandes_df

def load_data_to_snowflake(conn, dataframes):
    for df, table_name in dataframes:
        try:
            write_pandas(conn, df, table_name)
            print(f"Data loaded successfully into '{table_name}'")
        except Exception as e:
            print(f"Error loading data into '{table_name}': {e}")

def main():
    number_of_documents_to_display = 499
    mongo_hook = MongoHook(conn_id='mongoid')
    emails_collection = mongo_hook.get_conn().get_default_database().emails

    body_df = create_body_dataframe(emails_collection, number_of_documents_to_display)
    date_df, payment_df, restaurant_df, commandes_df = extract_and_transform_data(body_df)

    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowid')
    with snowflake_hook.get_conn() as conn:
        load_data_to_snowflake(conn, [
            (date_df, 'DATE_DIM'),
            (payment_df, 'PAIEMENT_DIM'),
            (restaurant_df, 'RESTAURANT_DIM'),
            (commandes_df, 'COMMANDES_FACT')
        ])

if __name__ == '__main__':
    main()
