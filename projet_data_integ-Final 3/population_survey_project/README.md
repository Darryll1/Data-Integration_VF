# Projet d'Intégration de Données

## Description du Projet
Ce projet est conçu pour gérer et intégrer des données issues d'enquêtes démographiques provenant de diverses sources, en surmontant des défis tels que les formats variés, les structures de données imprévisibles et les flux de données continus. 

Le projet intègre des données provenant de l'enquête communautaire américaine de 2015, en utilisant spécifiquement :

- Le jeu de données `total-population` (diffusé en continu via Kafka),
- `aggregate-household-income-in-the-past-12-months-in-2015-inflation-adjusted-dollars`,
- `types-of-health-insurance-coverage-by-age`,
- `self-employment-income-in-the-past-12-months-for-households`.

### Objectifs :
1. Recevoir et intégrer en continu les données `total-population` diffusées via Kafka.
2. Lire et intégrer les autres jeux de données stockés dans HDFS.
3. Gérer l'intégration des données avec une gestion des erreurs et la possibilité de revenir à des versions précédentes en cas d'incohérences.
4. Calculer et stocker des métriques significatives dans une base de données structurée.

## Prérequis
- Docker et Docker Compose
- Python 3.x
- Spark
- HDFS
## Ordre d'exécution 
## Produire les données avec Kafka : Exécutez le script producer.py pour envoyer des données dans le topic Kafka :
cd population_survey_project/kafka_producer
python producer.py

## Lire les données dans HDFS : Lancez hdfs_reader.py pour stocker les données dans HDFS :
cd ../hdfs_data
python hdfs_reader.py

## Consommer et traiter les données : Le script consumer.py consomme les données de Kafka et fait des jointures avec les données récupérées depuis HDFS et les traite via Spark Streaming :
cd ../spark_streaming
python consumer.py

## Calculer les métriques : Enfin, utilisez metrics_calculation.py pour générer des analyses basées sur les données traitées :
python metrics_calculation.py
