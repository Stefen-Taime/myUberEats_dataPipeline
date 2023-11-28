from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def create_tables():
    hook = SnowflakeHook(snowflake_conn_id='snowid')
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:        
        # Sélectionner l'entrepôt
        cursor.execute("USE WAREHOUSE COMPUTE_WH")
        # Création de la table restaurant_dim
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS restaurant_dim (
                "ID_Restaurant" VARCHAR(255) PRIMARY KEY,
                "NomRestaurant" VARCHAR(255)
            );
        """)
        print("Table 'restaurant_dim' créée avec succès.")

        # Création de la table paiement_dim
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS paiement_dim (
                "ID_Paiement" VARCHAR(255) PRIMARY KEY,
                "TypePaiement" VARCHAR(255),
                "Montant" DECIMAL(10, 2)
            );
        """)
        print("Table 'paiement_dim' créée avec succès.")

        # Création de la table date_dim
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

        # Création de la table commandes_fact
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

    except Exception as e:
        print(f"Erreur lors de la création des tables: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    create_tables()

if __name__ == "__main__":
    main()
