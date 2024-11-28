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


# API 

Cette API expose des métriques calculées à partir des données de la couche Gold via des points d'entrée RESTful. Elle est sécurisée à l'aide de **JSON Web Tokens (JWT)**.

## Configuration des URLs

Les URLs de l'API sont configurées dans le fichier `urls.py` comme suit :

```python
from django.urls import path
from .views import (
    TotalSalesPerProductView,
    StatusCodeCountsView,
    AvgLikesPerProductView,
    CampaignMetricsView,
)

urlpatterns = [
    path("total-sales-per-product/", TotalSalesPerProductView.as_view(), name="total_sales_per_product"),
    path("status-code-counts/", StatusCodeCountsView.as_view(), name="status_code_counts"),
    path("avg-likes-per-product/", AvgLikesPerProductView.as_view(), name="avg_likes_per_product"),
    path("campaign-metrics/", CampaignMetricsView.as_view(), name="campaign_metrics"),
]
```

## Points d'entrée disponibles

### 1. **Total des ventes par produit**
- **URL** : `/api/total-sales-per-product/`
- **Méthode HTTP** : `GET`
- **Description** : Renvoie le total des ventes pour chaque produit.
- **Exemple de réponse** :
```json
[
    {"produit": "Produit A", "total_ventes": 1500.0},
    {"produit": "Produit B", "total_ventes": 3200.0}
]
```

---

### 2. **Nombre de requêtes par code de statut HTTP**
- **URL** : `/api/status-code-counts/`
- **Méthode HTTP** : `GET`
- **Description** : Renvoie le nombre de requêtes pour chaque code de statut HTTP.
- **Exemple de réponse** :
```json
[
    {"status_code": 200, "count_requests": 150},
    {"status_code": 404, "count_requests": 25}
]
```

---

### 3. **Moyenne des likes par produit**
- **URL** : `/api/avg-likes-per-product/`
- **Méthode HTTP** : `GET`
- **Description** : Renvoie la moyenne des likes pour chaque produit.
- **Exemple de réponse** :
```json
[
    {"contenu": "Post A", "avg_likes": 75.3},
    {"contenu": "Post B", "avg_likes": 54.1}
]
```

---

### 4. **Métriques des campagnes publicitaires**
- **URL** : `/api/campaign-metrics/`
- **Méthode HTTP** : `GET`
- **Description** : Renvoie le total des clics et impressions pour chaque campagne.
- **Exemple de réponse** :
```json
[
    {"campaign_id": "Campagne 1", "total_clicks": 300, "total_impressions": 1200},
    {"campaign_id": "Campagne 2", "total_clicks": 450, "total_impressions": 1400}
]
```

---

## Authentification via JWT

Les points d'entrée sont sécurisés par **JSON Web Tokens (JWT)**. Vous devez inclure un token valide dans l'en-tête de chaque requête.

### Obtenir un token
- **URL** : `/api/token/`
- **Méthode HTTP** : `POST`
- **Exemple de corps de requête** :
```json
{
    "username": "votre_nom_utilisateur",
    "password": "votre_mot_de_passe"
}
```
- **Exemple de réponse** :
```json
{
    "access": "votre_token_access",
    "refresh": "votre_token_refresh"
}
```

### Rafraîchir un token
- **URL** : `/api/token/refresh/`
- **Méthode HTTP** : `POST`
- **Exemple de corps de requête** :
```json
{
    "refresh": "votre_token_refresh"
}
```
- **Exemple de réponse** :
```json
{
    "access": "nouveau_token_access"
}
```

---

## Tester l'API avec Postman

1. Configurez un environnement Postman avec l'URL de base : `http://127.0.0.1:8000/api/`.
2. Obtenez un token d'accès via l'URL `/api/token/`.
3. Ajoutez l'en-tête suivant à vos requêtes :
   ```
   Authorization: Bearer <votre_token_access>
   ```
4. Effectuez des requêtes `GET` vers les points d'entrée pour récupérer les données.


