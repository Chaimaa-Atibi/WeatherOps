import pandas as pd
from datetime import datetime
import json
import os

# --- Configuration des Chemins ---
# Le répertoire cible pour les données BRUTES validées
OUTPUT_DIR = 'data/raw' 
# Le chemin où vous aviez téléchargé le CSV original
CSV_SOURCE = '/data/weatherHistory.csv' 


def save_csv_to_raw():
    """
    1. Ingestion du fichier CSV (Global Weather Repository)
    2. Ajout du timestamp d'ingestion (Validation et Traçage)
    3. Sauvegarde dans la zone 'raw' du Data Lake.
    """
    CSV_DESTINATION = os.path.join(OUTPUT_DIR, 'weatherHistory_source.csv')
    
    print(f"\n[CSV Ingestion] Tentative de lecture du fichier source : {CSV_SOURCE}")

    try:
        # FETCH: Lecture du fichier CSV
        df = pd.read_csv(CSV_SOURCE)
        
        # VALIDATE: Ajout du timestamp d'ingestion (clé essentielle pour le pipeline)
        df['ingestion_timestamp'] = datetime.utcnow()
        
        # S'assurer que le répertoire de sortie existe
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # SAUVEGARDE: Sauvegarde du CSV validé dans la zone RAW
        df.to_csv(CSV_DESTINATION, index=False)
        print(f"  -> SUCCESS: Fichier CSV source (avec timestamp) sauvegardé dans {CSV_DESTINATION}")
        
    except FileNotFoundError:
        print(f"  -> AVERTISSEMENT : Le fichier source {CSV_SOURCE} n'a pas été trouvé. Cette étape est ignorée.")
    except Exception as e:
        print(f"  -> ERREUR : Une erreur est survenue lors du traitement du CSV : {e}")


def simulate_api_call():
    """
    1. Simule l'appel d'une API météo (logique moderne).
    2. Ajout du timestamp d'ingestion.
    3. Sauvegarde en JSON (ou Parquet) dans la zone 'raw'.
    """
    # Données JSON simulées pour démontrer le processus sans clé API réelle
    raw_data = [
        {'city': 'Rabat', 'temp_c': 20.1, 'humidity': 75, 'wind_speed': 5.5, 'date': '2025-12-14 12:00:00'},
        {'city': 'Paris', 'temp_c': 6.3, 'humidity': 88, 'wind_speed': 10.2, 'date': '2025-12-14 13:00:00'},
        {'city': 'Casablanca', 'temp_c': 18.0, 'humidity': 70, 'wind_speed': 4.0, 'date': '2025-12-14 14:00:00'}
    ]
    
    print("\n[API Ingestion] Simulation de l'appel d'une API météo...")
    
    # Étape VALIDATE : Ajouter le timestamp à tous les enregistrements
    ingestion_timestamp = datetime.utcnow().isoformat() + 'Z'
    validated_api_data = []
    
    for record in raw_data:
        record['ingestion_timestamp'] = ingestion_timestamp # Ajout du traçage
        validated_api_data.append(record)
        
    print("  -> Validation réussie : Timestamp ajouté aux données API.")
        
    # Étape SAUVEGARDE : Sauvegarder les données API en JSON
    output_filename = 'weather_api_' + datetime.now().strftime("%Y%m%d%H%M%S") + '.json'
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    
    try:
        with open(output_path, 'w') as f:
            json.dump(validated_api_data, f, indent=4)
        
        print(f"  -> SUCCESS: Fichier JSON API sauvegardé dans {output_path}")
    except Exception as e:
        print(f"  -> ERREUR : Impossible d'écrire le fichier JSON : {e}")


def run_ingestion_pipeline():
    """ Fonction principale pour exécuter l'Étape 2 du pipeline. """
    print("==================================================")
    print("⭐ Démarrage de l'Étape 2 : Python Ingestion Script (Fetch, Validate, Save)")
    
    # Exécuter les deux sources d'ingestion
    save_csv_to_raw()
    simulate_api_call()
    
    print("\n✅ Étape 2 terminée. Les données brutes sont dans le dossier data/raw.")
    print("Prochaine étape : Data Cleaning & Transformation (Étape 5)")
    print("==================================================")


if __name__ == "__main__":
    run_ingestion_pipeline()