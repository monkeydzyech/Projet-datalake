# Projet de Data Lake et Médaillon Architecture

Ce projet implémente une architecture de Data Lake avec trois couches de transformation des données : **Bronze**, **Silver**, et **Gold**. Chaque couche correspond à une étape spécifique du traitement des données, allant de l'ingestion brute à des métriques agrégées et prêtes pour l'analyse.

## Description des couches

1. **Bronze (Ingestion des données brutes)** :
   - La couche Bronze contient les données brutes qui sont ingérées à partir de diverses sources (CSV, logs, JSON, Kafka).
   - Ces données sont stockées dans leur format d'origine sans nettoyage ou transformation.

2. **Silver (Transformation et nettoyage)** :
   - La couche Silver contient les données nettoyées et transformées. Les valeurs manquantes sont traitées, les types de données sont ajustés, et certaines données sont filtrées.
   - Cette couche est utilisée pour transformer les données brutes en un format plus structuré et utilisable.

3. **Gold (Calcul des métriques et agrégations)** :
   - La couche Gold contient les métriques agrégées et les résultats prêts pour l'analyse.
   - Dans cette couche, les calculs comme les moyennes, les sommes et autres métriques statistiques sont effectués sur les données des couches précédentes.

## Structure du projet

Le projet est composé des scripts suivants, qui exécutent les étapes de transformation et de calcul des métriques.

### 1. **bronze.py** : Ingestion des données dans la couche Bronze
Ce script ingère les données brutes depuis des fichiers locaux et les charge dans le système HDFS dans le répertoire `bronze`.

### 2. **silver.py** : Transformation et nettoyage des données
Ce script nettoie et transforme les données brutes de la couche Bronze. Il traite les valeurs manquantes, filtre les données incorrectes, et formate les données.

### 3. **gold.py** : Calcul des métriques dans la couche Gold
Le script `gold.py` agrège les données nettoyées et calcul les métriques nécessaires à partir des données des couches précédentes.

### 4. **visualisation_silver.py** : Visualisation des données nettoyées de la couche Silver
Ce script permet de visualiser les données nettoyées et transformées dans la couche Silver.

### 5. **visualisation_gold.py** : Visualisation des métriques agrégées dans la couche Gold
Ce script permet de visualiser les résultats calculés dans la couche Gold (métriques agrégées).

### 6. **producer.py** : Producteur Kafka pour les flux de données en temps réel
Ce script génère des flux de données en temps réel pour alimenter le Data Lake à partir de Kafka.

## Installation et Prérequis

1. **Apache Spark** :
   - Téléchargez et installez Apache Spark sur votre machine. Assurez-vous que Spark est configuré pour fonctionner avec Hadoop (HDFS).

2. **Kafka** :
   - Kafka est utilisé pour simuler des flux de données en temps réel. Assurez-vous que Kafka est en cours d'exécution sur votre machine locale.
   
3. **Hadoop HDFS** :
   - Hadoop HDFS est utilisé comme système de stockage distribué pour stocker les données dans les répertoires `bronze`, `silver`, et `gold`.
   
4. **Python et PySpark** :
   - Installez Python 3.x et la bibliothèque PySpark pour interagir avec Spark via Python.
   - Vous pouvez installer PySpark avec la commande :
     ```bash
     pip install pyspark
     ```

## Flux de travail du projet

### Étape 1 : Ingestion des données brutes (Bronze Layer)

1. **Transactions Clients** : Les données de transactions sont lues à partir d'un fichier CSV local et chargées dans HDFS.
2. **Logs Serveur** : Les logs des serveurs sont ingérés à partir d'un fichier texte local et stockés dans HDFS.
3. **Données Sociales** : Les données sociales sont ingérées depuis un fichier JSON local.
4. **Flux Publicitaire** : Des données de flux publicitaires sont lues à partir de Kafka et stockées dans HDFS.

### Étape 2 : Transformation des données (Silver Layer)

1. Les données dans la couche Bronze sont nettoyées :
   - Les valeurs manquantes sont traitées.
   - Les types de données sont ajustés.
   - Les données invalides sont filtrées.

### Étape 3 : Calcul des métriques (Gold Layer)

1. **Calcul des ventes totales par produit** : Total des ventes par produit basé sur la quantité et le prix.
2. **Nombre de clics et impressions par campagne publicitaire** : Calcul du nombre de clics et impressions pour chaque campagne.
3. **Statistiques des codes de statut des logs** : Comptage des occurrences de chaque code de statut des logs des serveurs.
4. **Calcul de la moyenne des likes par produit** : Moyenne des likes pour chaque produit dans les posts sociaux.

### Étape 4 : Visualisation

1. **Visualisation des données nettoyées (Silver)** : Les données nettoyées et transformées dans la couche Silver sont visualisées pour comprendre les résultats de transformation.
2. **Visualisation des métriques (Gold)** : Les métriques calculées dans la couche Gold sont affichées pour donner un aperçu des performances commerciales.

## Exécution des scripts

### Exécution des scripts via Spark
Apres avoir cree le prodcuer et envoye les données dans le topics.

1. **Ingestion des données dans Bronze** :
   ```bash
   spark-submit --master "local[*]" bronze.py

2. **Transformation des données dans Silver
spark-submit --master "local[*]" silver.py

3. Calcul des métriques dans Gold :
spark-submit --master "local[*]" gold.py

4. Visualisation des données Silver 
spark-submit --master "local[*]" visualisation_silver.py

5. Visualisation des données Gold :
spark-submit --master "local[*]" visualisation_gold.py

