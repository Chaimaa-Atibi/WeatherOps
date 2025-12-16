import os
from google.cloud import bigquery
from google.oauth2 import service_account

# --- Configuration du Projet et des Chemins ---
# 1. AUTHENTIFICATION : Chemin vers la clé de service JSON (Maintenant OK)
CREDENTIAL_FILE = 'gcp_key.json' 
# 2. SOURCE : Fichier Parquet nettoyé (Étape 6)
INPUT_FILE_PATH = 'data/processed/weather_processed_data.parquet'

# 3. DESTINATION BIGQUERY : Configurez vos identifiants GCP
PROJECT_ID = 'wagon-bootcamp-470119' 
DATASET_ID = 'weather_data_lake'  # Nom du jeu de données (Dataset) BigQuery à créer
TABLE_ID = 'weather_forecast_history' # Nom de la table qui sera créée ou mise à jour


def load_parquet_to_bigquery():
    """
    Lit le fichier Parquet et charge les données dans BigQuery en utilisant
    le fichier de clé de compte de service.
    """
    print("1. Démarrage de la connexion à BigQuery...")

    # A. AUTHENTIFICATION
    try:
        # Charge les identifiants depuis le fichier JSON
        credentials = service_account.Credentials.from_service_account_file(CREDENTIAL_FILE)
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        print(f"  -> Authentification réussie pour le projet: {PROJECT_ID}")
    except Exception as e:
        print(f"  -> ERREUR d'authentification. Assurez-vous que {CREDENTIAL_FILE} est valide et à la racine. Détail: {e}")
        return

    # B. PRÉPARATION DE LA DESTINATION
    dataset_ref = client.dataset(DATASET_ID)
    table_ref = dataset_ref.table(TABLE_ID)

    # 1. Tente de créer le Dataset s'il n'existe pas
    try:
        client.get_dataset(dataset_ref)
        print(f"  -> Dataset '{DATASET_ID}' existe déjà.")
    except Exception:
        print(f"  -> Création du Dataset '{DATASET_ID}'...")
        client.create_dataset(bigquery.Dataset(dataset_ref))
        print(f"  -> Dataset '{DATASET_ID}' créé avec succès.")

    # C. CONFIGURATION DU CHARGEMENT (Job Configuration)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True, 
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # D. CHARGEMENT
    print(f"\n2. Chargement du fichier '{INPUT_FILE_PATH}' vers BigQuery...")
    
    try:
        with open(INPUT_FILE_PATH, "rb") as source_file:
            job = client.load_table_from_file(
                source_file,
                table_ref,
                job_config=job_config,
            )

        job.result() 

        # E. VÉRIFICATION
        print("\n3. Vérification des résultats.")
        table = client.get_table(table_ref) 
        print(f"  -> Chargement terminé. Table '{TABLE_ID}' créée dans BigQuery.")
        print(f"  -> Nombre de lignes chargées : {table.num_rows}")
        print("\n" + "="*50)
        print("FÉLICITATIONS ! Étape 7 (Data Warehouse) complétée.")
        print("Prochaine étape : Data Orchestration (Airflow).")
        print("="*50)

    except FileNotFoundError:
        print(f"  -> ERREUR: Le fichier source Parquet {INPUT_FILE_PATH} est introuvable. Avez-vous exécuté l'Étape 6 (transformation) ?")
    except Exception as e:
        print(f"  -> ERREUR lors du chargement BigQuery : {e}")


if __name__ == "__main__":
    load_parquet_to_bigquery()