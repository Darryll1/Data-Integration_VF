from pyspark.sql import SparkSession
import os
import shutil
import json


# Initialiser SparkSession
spark = SparkSession.builder.appName("HDFS Data Reader").getOrCreate()

def load_data_from_path(path, file_format="csv", **options):
    """Charge les données à partir d'un chemin donné."""
    return spark.read.format(file_format).options(**options).load(path)

# Chemins des fichiers et options


base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir, "sources_config.json")

with open(config_path, "r") as f:
    paths = json.load(f)

# with open("sources_config.json", "r") as f:
#     paths = json.load(f)


# Charger les datasets
dataframes = {name: load_data_from_path(info["path"], file_format=info["format"], **info["options"]) 
              for name, info in paths.items()}

# Renommer et nettoyer les colonnes des datasets
for name, df in dataframes.items():
    print(f"Schema original pour {name}:")
    df.printSchema()
    
    # Ajouter un préfixe unique aux colonnes pour éviter les conflits
    prefix = name + "_"
    for col in df.columns:
        df = df.withColumnRenamed(col, prefix + col.replace(" ", "_").replace(";", "").replace(":", "").replace("-", "_"))
    
    # Sauvegarder le dataframe nettoyé
    dataframes[name] = df

    print(f"Nouveau schema pour {name}:")
    df.printSchema()

# Jointure des trois datasets
if "aggregate_income" in dataframes and "health_insurance" in dataframes and "self_employment_income" in dataframes:
    # Jointure sur la clé commune (exemple : aggregate_income_Id, health_insurance_Id, self_employment_income_Id)
    joined_df = dataframes["aggregate_income"] \
        .join(dataframes["health_insurance"], on=[dataframes["aggregate_income"]["aggregate_income_Id"] == dataframes["health_insurance"]["health_insurance_Id"]], how="inner") \
        .join(dataframes["self_employment_income"], on=[dataframes["aggregate_income"]["aggregate_income_Id"] == dataframes["self_employment_income"]["self_employment_income_Id"]], how="inner") \
        .drop("health_insurance_Id", "self_employment_income_Id")  # Supprimer les colonnes redondantes

    print("Schema après jointure des trois datasets:")
    joined_df.printSchema()

    # Afficher un échantillon des données jointes
    joined_df.show(10)

    # Sauvegarder le dataset joint
 

# Répertoire temporaire pour le fichier CSV
temp_output_dir = "temp_all_data_output"
final_output_file = "all_data_joined.csv"

# Réduire à une seule partition si nécessaire (facultatif si petit dataset)
joined_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_output_dir)

# Localiser le fichier CSV généré dans le répertoire temporaire et le renommer
for file_name in os.listdir(temp_output_dir):
    if file_name.endswith(".csv"):
        os.rename(os.path.join(temp_output_dir, file_name), final_output_file)
        break

# Supprimer le répertoire temporaire
shutil.rmtree(temp_output_dir)

print(f"Dataset joint sauvegardé sous le nom {final_output_file}")


