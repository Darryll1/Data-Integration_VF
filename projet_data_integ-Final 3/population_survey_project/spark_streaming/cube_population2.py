import sqlite3
import pandas as pd

# Connexion à la nouvelle base SQLite
conn = sqlite3.connect("/app/population_survey_project\spark_streaming\cube_analytics.db")

# Connexion à la base de données source
source_conn = sqlite3.connect("/app/population_survey_project\spark_streaming\base_de_donnees.db")
df = pd.read_sql_query("SELECT * FROM donnees_formatées", source_conn)
source_conn.close()

# Suppression des colonnes de marge d'erreur
df = df.loc[:, ~df.columns.str.contains('Margin_of_Error')]

# Définition des cubes à créer
categories = {
    "cube_health_18_34": "health_insurance_Estimate_18_to_34_years",
    "cube_health_35_64": "health_insurance_Estimate_35_to_64_years",
    "cube_health_65_plus": "health_insurance_Estimate_65_years_and_over",
    "cube_self_employment": "self_employment_income_Estimate",
    "cube_income": "aggregate_income_Estimate"
}

# Création de chaque cube
for cube_name, prefix in categories.items():
    mesure_cols = [col for col in df.columns if col.startswith(prefix)]

    if not mesure_cols:
        print(f"Aucune colonne de mesure trouvée pour {cube_name}.")
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
        # Conversion en float pour permettre les moyennes
        df[mesure_cols] = df[mesure_cols].apply(pd.to_numeric, errors='coerce')

        # Calcul du cube
        cube_df = df.groupby(group_col)[mesure_cols].mean().reset_index()

        # Enregistrement dans la base cible
        cube_df.to_sql(cube_name, conn, if_exists='replace', index=False)
        print(f"Cube {cube_name} enregistré dans 'cube_analytics.db'.")

    except Exception as e:
        print(f"Erreur lors de l'enregistrement de {cube_name} : {e}")

# Fermeture de la connexion
conn.close()

