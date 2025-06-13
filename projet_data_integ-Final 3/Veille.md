
# Veille Technologique – Frameworks Big Data (C1)

## Objectif de la veille
Dans le cadre du projet d’intégration de données démographiques, une veille technologique a été réalisée afin d’identifier, comparer et sélectionner les frameworks Big Data les plus adaptés au traitement distribué de données massives, à l’ingestion de flux en temps réel et à l’analytique en batch. Cette veille a permis de justifier les choix technologiques retenus dans l’architecture du projet.


## Contrôle de version
- Utilisation de Git et GitHub pour tracer les modifications et faciliter la collaboration.

## Containerisation
- Docker + Docker Compose pour isoler les services (Kafka, Spark, Airflow, Prometheus, etc.).

## 1. Apache Hadoop

### Présentation :
Apache Hadoop est un framework open-source qui permet le stockage distribué (via HDFS) et le traitement parallèle de grandes quantités de données à l’aide de MapReduce.

### Avantages :
- Écosystème mature et robuste.
- Haute tolérance aux pannes grâce à la réplication des données dans HDFS.
- Intégration facile avec d'autres outils (Hive, Pig, HBase).

### Inconvénients :
- Moins performant pour les traitements interactifs.
- Configuration complexe.

### Cas d’usage :
Utilisé ici comme système de stockage principal pour les fichiers CSV de données statiques.

---

## 2. Apache Kafka

### Présentation :
Kafka est une plateforme de streaming distribuée permettant l’ingestion, la publication et la souscription à des flux de données en temps réel.

### Avantages :
- Faible latence et haut débit.
- Très résilient (réplication, tolérance aux pannes).
- Bonne intégration avec Spark, Flink, etc.

### Inconvénients :
- Complexité de configuration initiale.
- Nécessite une bonne surveillance opérationnelle.

### Cas d’usage :
Kafka est utilisé pour capter et transmettre en temps réel le flux de population (`total-population`) au moteur de traitement.

---

## 3. Apache Spark

### Présentation :
Spark est un moteur de traitement de données massives qui permet des traitements en batch, en temps réel, ainsi que de l’analyse en mémoire (in-memory computing).

### Avantages :
- Très rapide grâce au traitement en mémoire.
- Support du batch, streaming, machine learning et SQL.
- API unifiée pour Scala, Java, Python (PySpark).

### Inconvénients :
- Peut être gourmand en mémoire.
- Configuration plus complexe que Hadoop seul.

### Cas d’usage :
Recommandé pour les traitements analytiques et la jointure des données HDFS + flux Kafka.

--

## Conclusion de la veille

La combinaison **Hadoop + Kafka + Spark**, déployée via Docker, représente une architecture cohérente, performante et résiliente pour répondre aux exigences du projet. Chaque technologie a été choisie pour son rôle optimal dans l’écosystème Big Data, garantissant ainsi une ingestion efficace, un stockage fiable et un traitement distribué performant.
