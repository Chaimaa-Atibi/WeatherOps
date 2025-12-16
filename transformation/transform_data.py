import pandas as pd
import glob
import os

# --- Configuration des Chemins ---
INPUT_DIR = 'data/raw'
OUTPUT_DIR = 'data/processed'
OUTPUT_FILE_NAME = 'weather_processed_data.parquet'
OUTPUT_FILE_PATH = os.path.join(OUTPUT_DIR, OUTPUT_FILE_NAME)


def load_raw_data():
    """
    Charge les différents fichiers ingérés (CSV et JSON) depuis la zone 'raw'.
    """
    all_data = []

    # 1. Charger le fichier CSV (Source historique)
    csv_path = os.path.join(INPUT_DIR, 'weatherHistory_source.csv')
    try:
        df_csv = pd.read_csv(csv_path)
        print(f"  -> Chargé le fichier CSV : {len(df_csv)} lignes.")
        # Nettoyage minimal spécifique au CSV
        # Sélectionner les colonnes pertinentes et harmoniser les noms
        df_csv.rename(columns={'Formatted Date': 'datetime_utc', 
                                'Summary': 'weather_summary',
                                'Temperature (C)': 'temp_c',
                                'Humidity': 'humidity',
                                'Wind Speed (km/h)': 'wind_speed_kmh',
                                'Pressure (millibars)': 'pressure_mbar'}, inplace=True)
        # Conserver les colonnes d'intérêt
        df_csv = df_csv[['datetime_utc', 'temp_c', 'humidity', 'wind_speed_kmh', 'pressure_mbar', 'weather_summary', 'ingestion_timestamp']]
        
        all_data.append(df_csv)
    except FileNotFoundError:
        print(f"  -> AVERTISSEMENT : Fichier CSV {csv_path} non trouvé. Ignoré.")
    except Exception as e:
        print(f"  -> ERREUR lors du chargement du CSV : {e}")


    # 2. Charger les fichiers JSON (Source API)
    # glob permet de trouver tous les fichiers JSON dans le répertoire
    json_files = glob.glob(os.path.join(INPUT_DIR, 'weather_api_*.json'))
    
    for file in json_files:
        try:
            df_json = pd.read_json(file)
            # Nettoyage minimal spécifique au JSON
            # Harmonisation des noms de colonnes du JSON simulé
            df_json.rename(columns={'date': 'datetime_utc',
                                    'temp_c': 'temp_c'}, inplace=True)
            # Ajout des colonnes manquantes pour harmonisation avec le CSV
            df_json['pressure_mbar'] = None # Remplissage par None pour l'alignement
            df_json['wind_speed_kmh'] = None
            df_json['weather_summary'] = 'API Forecast'
            
            df_json = df_json[['datetime_utc', 'temp_c', 'humidity', 'wind_speed_kmh', 'pressure_mbar', 'weather_summary', 'ingestion_timestamp']]

            all_data.append(df_json)
            print(f"  -> Chargé le fichier JSON : {len(df_json)} lignes.")
        except Exception as e:
            print(f"  -> ERREUR lors du chargement de {file}: {e}")
            
    # Concaténer tous les DataFrames chargés
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        print("  -> ERREUR FATALE : Aucune donnée brute n'a pu être chargée.")
        return None


def clean_and_transform(df):
    """
    Applique les règles de nettoyage et d'enrichissement.
    """
    if df is None:
        return None

    print(f"\n2. Transformation des données. Total initial : {len(df)} lignes.")
    
    # 2.1. CLEANING : Gestion des dates et valeurs manquantes
    # Conversion de la colonne date en type datetime
    df['datetime_utc'] = pd.to_datetime(df['datetime_utc'], utc=True, errors='coerce')
    
    # Supprimer les lignes où la date n'a pas pu être parsée
    df.dropna(subset=['datetime_utc'], inplace=True)
    
    # Supprimer les lignes où la température est manquante (colonne clé)
    df.dropna(subset=['temp_c'], inplace=True)

    # Remplir les valeurs manquantes pour l'humidité et la pression avec la moyenne
    df['humidity'].fillna(df['humidity'].mean(), inplace=True)
    df['pressure_mbar'].fillna(df['pressure_mbar'].mean(), inplace=True)
    
    print(f"  -> Nettoyage effectué. Lignes restantes : {len(df)}")
    
    # 2.2. ENRICHISSEMENT : Création de colonnes analytiques
    df['date_day'] = df['datetime_utc'].dt.date # Pour partitionnement ou agrégation future
    
    # CORRECTION CRITIQUE : Convertir l'objet 'date' en string pour la compatibilité Parquet
    df['date_day'] = df['date_day'].astype(str)
    
    df['temp_fahrenheit'] = (df['temp_c'] * 9/5) + 32 # Conversion utile pour certaines analyses
    
    print("  -> Enrichissement effectué : 'date_day' (corrigée pour Parquet) et 'temp_fahrenheit' ajoutées.")
    
    return df


def save_to_curated_data_lake(df):
    """
    Sauvegarde le DataFrame transformé au format Parquet dans la zone 'processed/curated'.
    """
    if df is not None:
        print(f"\n3. Sauvegarde des données transformées vers : {OUTPUT_FILE_PATH}")
        
        # Créer le répertoire de sortie s'il n'existe pas
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Sauvegarde au format Parquet (OPTIMAL pour le Data Lake et BigQuery)
        df.to_parquet(OUTPUT_FILE_PATH, index=False)
        
        print("  -> Sauvegarde réussie au format Parquet.")
        print("-" * 50)
        print("Prochaine étape : Data Warehousing avec BigQuery (Étape 7).")


if __name__ == "__main__":
    # S'assurer que le Data Lake (zone processed) est prêt
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Étape 1 : Chargement et harmonisation des données
    combined_raw_data = load_raw_data()
    
    # Étape 2 : Nettoyage et transformation
    processed_data = clean_and_transform(combined_raw_data)
    
    # Étape 3 : Sauvegarde dans la zone Curated/Processed
    save_to_curated_data_lake(processed_data)