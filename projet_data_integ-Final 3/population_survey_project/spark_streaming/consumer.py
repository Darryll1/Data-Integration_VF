from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import sqlite3
import pandas as pd
from tabulate import tabulate
import time
import os

from prometheus_client import Counter, Histogram, start_http_server

# === Prometheus metrics ===
start_http_server(8000)
MESSAGE_COUNT = Counter('processed_messages_total', 'Nombre total de messages traités')
ERROR_COUNT = Counter('processing_errors_total', 'Nombre total d\'erreurs de traitement')
PROCESSING_TIME = Histogram('message_processing_duration_seconds', 'Temps de traitement de chaque message')

# === Kafka setup ===
KAFKA_BROKER = 'kafka:9093'  # NE PAS utiliser localhost ici
TOPIC = 'population_data'



db_path = "/app/population_survey_project/spark_streaming/base_de_donnees.db"

# 1. Tenter d’ouvrir la base (optionnel : crée le fichier s’il n’existe pas)
conn = sqlite3.connect(db_path)

# 2. Fermer explicitement la connexion pour libérer le verrou OS
conn.close()

# 3. Vérifier l’existence du fichier et le supprimer
if os.path.exists(db_path):
    os.remove(db_path)
    print(f"✅ Base de données « {db_path} » supprimée.")
else:
    print(f"ℹ️  Aucune base de données nommée « {db_path} » n’a été trouvée.")




# === Tentatives de connexion à Kafka ===
consumer = None
for attempt in range(10):
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        print(f"✅ Connecté à Kafka après {attempt + 1} tentative(s).")
        break
    except NoBrokersAvailable:
        print(f"⚠️ Kafka non disponible, tentative {attempt + 1}/10... nouvelle tentative dans 5s")
        time.sleep(5)

if consumer is None:
    raise Exception("❌ Échec de la connexion à Kafka après 10 tentatives.")

print(f"🎧 Écoute des messages sur le topic '{TOPIC}'...")

# === Traitement des messages Kafka ===
while True:
    try:
        for message in consumer:
            with PROCESSING_TIME.time():
                try:
                    json_data = message.value
                    json_df = pd.DataFrame([json_data])
                    json_df['Id'] = json_df['Id'].astype(int)

                    # Chargement CSV
                    csv_path = "/app/population_survey_project/hdfs_data/all_data_joined.csv"
                    if not os.path.exists(csv_path):
                        print(f"⚠️ Fichier introuvable : {csv_path}")
                        continue

                    csv_df = pd.read_csv(csv_path)
                    print(f"all_data_joined.csv a {csv_df.shape[0]} lignes et {csv_df.shape[0]} colonnes")
                    csv_df['aggregate_income_Id'] = csv_df['aggregate_income_Id'].astype(int)

                    # Jointure
                    merged_df = pd.merge(
                        json_df,
                        csv_df,
                        left_on="Id",
                        right_on="aggregate_income_Id",
                        how="inner"
                    )

                    # Nettoyage
                    for col in merged_df.columns:
                        if merged_df[col].dtype == 'object':
                            merged_df[col] = merged_df[col].astype(str)
                        elif merged_df[col].dtype in ['int64', 'float64']:
                            merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')

                    if 'Id' in merged_df.columns:
                        merged_df = merged_df.dropna(subset=['Id'])

                    # Enregistrement dans SQLite
                    try:
                        conn = sqlite3.connect('base_de_donnees.db')
                        merged_df.to_sql('donnees_formatées', conn, if_exists='append', index=False)

                        preview = pd.read_sql_query("SELECT COUNT(*) FROM donnees_formatées", conn)
                        print(tabulate(preview, headers='keys', tablefmt='psql'))

                    except Exception as db_err:
                        print(f"[Erreur SQLite] {db_err}")
                        ERROR_COUNT.inc()
                    finally:
                        conn.close()

                    MESSAGE_COUNT.inc()

                except Exception as msg_err:
                    print(f"[Erreur traitement message] {msg_err}")
                    ERROR_COUNT.inc()

    except Exception as kafka_err:
        print(f"[Erreur Kafka] {kafka_err}")
        print("Reconnexion dans 5 secondes...")
        time.sleep(5)
