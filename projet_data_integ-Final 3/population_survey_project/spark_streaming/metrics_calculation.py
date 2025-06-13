import sqlite3
import pandas as pd
from tabulate import tabulate

# Connexion à la base de données existante
source_db = '/app/population_survey_project\spark_streaming\ma_base_de_donnees8.db'
conn_source = sqlite3.connect(source_db)

# Charger les données de la table source dans un DataFrame
query = "SELECT * FROM ma_table"
data_df = pd.read_sql_query(query, conn_source)

# Vérifier les noms des colonnes
print("Colonnes disponibles :", data_df.columns)

# Vérifier le type et les données dans la colonne problématique
col_name = 'aggregate_income_Estimate_Aggregate_household_income_in_the_past_12_months_(in_2015_Inflation_adjusted_dollars)'
if col_name not in data_df.columns:
    raise KeyError(f"La colonne {col_name} n'existe pas dans les données.")

print(f"Type des données dans {col_name} :", data_df[col_name].dtype)
print(f"Exemples de valeurs dans {col_name} :", data_df[col_name].unique())

# Convertir la colonne en type numérique
data_df[col_name] = pd.to_numeric(data_df[col_name], errors='coerce')

# Gérer les valeurs NaN
print(f"Valeurs manquantes dans {col_name} avant traitement :", data_df[col_name].isna().sum())
data_df[col_name].fillna(0, inplace=True)  # Remplacer les NaN par 0

# Vérifier les autres colonnes utilisées pour les calculs
if 'Estimate; Total' not in data_df.columns:
    raise KeyError("La colonne 'Estimate; Total' n'existe pas dans les données.")

if 'Neighborhood' not in data_df.columns:
    raise KeyError("La colonne 'Neighborhood' n'existe pas dans les données.")

# Calculer des métriques significatives
metrics = {
    "Total Population": data_df['Estimate; Total'].sum(),
    "Average Income": data_df[col_name].mean(),
    "Median Income": data_df[col_name].median(),
    "Number of Records": len(data_df),
    "Income by Region": data_df.groupby('Neighborhood')[col_name].sum().to_dict()
}

# Transformer les métriques en DataFrame pour enregistrement
metrics_df = pd.DataFrame([
    {"Metric": key, "Value": str(value)} for key, value in metrics.items()
])

# Fermer la connexion à la base de données source
conn_source.close()

# Enregistrer les métriques dans une nouvelle base de données
metrics_db = 'metrics_database.db'
conn_metrics = sqlite3.connect(metrics_db)

# Insérer les métriques dans une table
metrics_df.to_sql('metrics_table', conn_metrics, if_exists='append', index=False)

# Afficher les métriques calculées
print("Métriques calculées :")
print(tabulate(metrics_df, headers='keys', tablefmt='psql'))

# Fermer la connexion à la base de données des métriques
conn_metrics.close()
