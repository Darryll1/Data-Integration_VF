import sqlite3
import pandas as pd
import os
import time

# === Chemins ===
base_path = "/app/population_survey_project/spark_streaming"
db_source_path = os.path.join(base_path, "base_de_donnees.db")
db_target_path = os.path.join(base_path, "cube_analytics.db")

# === Attente de la création de la base de données source ===
max_wait_time = 60
elapsed = 0
wait_interval = 5

print(f"⏳ Attente de la base de données : {db_source_path}")

while not os.path.exists(db_source_path):
    if elapsed >= max_wait_time:
        raise TimeoutError(f"❌ La base {db_source_path} n'a pas été trouvée après {max_wait_time} secondes.")
    time.sleep(wait_interval)
    elapsed += wait_interval
    print(f"🔁 Toujours en attente... ({elapsed}s)")

print("✅ Base de données trouvée. Lancement du traitement continu.")

# === Définition des catégories pour les cubes ===
categories = {
    "cube_health_18_34": "health_insurance_Estimate_18_to_34_years",
    "cube_health_35_64": "health_insurance_Estimate_35_to_64_years",
    "cube_health_65_plus": "health_insurance_Estimate_65_years_and_over",
    "cube_self_employment": "self_employment_income_Estimate",
    "cube_income": "aggregate_income_Estimate"
}

# === Boucle infinie de recalcul des cubes ===
while True:
    try:
        source_conn = sqlite3.connect(db_source_path)
        df = pd.read_sql_query("SELECT * FROM donnees_formatées", source_conn)
        source_conn.close()

        if df.empty:
            print("⚠️ Pas de données encore disponibles, nouvelle tentative dans 10s...")
            time.sleep(10)
            continue

        df = df.loc[:, ~df.columns.str.contains('Margin_of_Error')]

        conn = sqlite3.connect(db_target_path)

        for cube_name, prefix in categories.items():
            mesure_cols = [col for col in df.columns if col.startswith(prefix)]

            if not mesure_cols:
                print(f"⚠️ Aucune colonne de mesure trouvée pour {cube_name}.")
                continue

            if 'health_insurance' in prefix:
                group_col = 'health_insurance_Neighborhood'
            elif 'self_employment' in prefix:
                group_col = 'self_employment_income_Neighborhood'
            elif 'aggregate_income' in prefix:
                group_col = 'aggregate_income_Neighborhood'
            else:
                print(f"⚠️ Colonne de regroupement inconnue pour {cube_name}")
                continue

            try:
                df[mesure_cols] = df[mesure_cols].apply(pd.to_numeric, errors='coerce')
                cube_df = df.groupby(group_col)[mesure_cols].mean().reset_index()
                cube_df.to_sql(cube_name, conn, if_exists='replace', index=False)
                print(f"✅ Cube {cube_name} mis à jour ({len(cube_df)} lignes).")

            except Exception as e:
                print(f"❌ Erreur pour {cube_name} : {e}")

        conn.close()

    except Exception as loop_err:
        print(f"❌ Erreur dans la boucle principale : {loop_err}")

    print("⏳ Pause de 30s avant prochaine mise à jour...")
    time.sleep(30)  # Recalcul toutes les 30 secondes
