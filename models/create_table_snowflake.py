import snowflake.connector
from dotenv import load_dotenv
import os

# Charger les variables d'environnement
load_dotenv()

# Récupérer les informations de connexion
user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
account = os.getenv("SNOWFLAKE_ACCOUNT")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
database = os.getenv("SNOWFLAKE_DATABASE")
schema = os.getenv("SNOWFLAKE_SCHEMA")

# Établir la connexion
conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema
)

try:
    cursor = conn.cursor()

    # Sélectionner l'entrepôt
    cursor.execute(f"USE WAREHOUSE {warehouse}")

    # Créer les tables nécessaires

    # Table restaurant_dim
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS restaurant_dim (
        "ID_Restaurant" VARCHAR(255) PRIMARY KEY,
        "NomRestaurant" VARCHAR(255)
        );
    """)
    print("Table 'restaurant_dim' créée avec succès.")

    # Table paiement_dim
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS paiement_dim (
        "ID_Paiement" VARCHAR(255) PRIMARY KEY,
        "TypePaiement" VARCHAR(255),
        "Montant" DECIMAL(10, 2)
        );
    """)
    print("Table 'paiement_dim' créée avec succès.")

    # Ajout de la table date_dim
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS date_dim (
        "DateHeure" VARCHAR(255) PRIMARY KEY,
        "Jour" INTEGER,
        "Mois" INTEGER,
        "Annee" INTEGER,
        "Heure" INTEGER,
        "Minute" INTEGER
        );
    """)
    print("Table 'date_dim' créée avec succès.")

 # Créer la table de faits Commandes
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS commandes_fact (
            "ID_Commande" VARCHAR(255) PRIMARY KEY,
            "ID_Restaurant" VARCHAR(255),
            "ID_Paiement" VARCHAR(255),
            "ID_DateHeure" VARCHAR(255),
            "MontantTotal" DECIMAL(10, 2),
            FOREIGN KEY ("ID_Restaurant") REFERENCES restaurant_dim("ID_Restaurant"),
            FOREIGN KEY ("ID_Paiement") REFERENCES paiement_dim("ID_Paiement"),
            FOREIGN KEY ("ID_DateHeure") REFERENCES date_dim("DateHeure")
        );
    """)
    print("Table 'commandes_fact' créée avec succès.")

except snowflake.connector.errors.ProgrammingError as e:
    print(f"Erreur lors de la création de la table: {e}")
    # Tester la connexion
    cursor.execute("SELECT CURRENT_VERSION()")
    one_row = cursor.fetchone()
    print(f"Connecté à Snowflake. Version: {one_row[0]}")
finally:
    cursor.close()
    conn.close()
