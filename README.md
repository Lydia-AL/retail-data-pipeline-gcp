# Retail Data Pipelines on Google Cloud

# Pipelines de données Retail sur Google Cloud

Ce dépôt présente plusieurs pipelines de Data Engineering construits sur Google Cloud autour d'un cas d'usage dans le domaine du retail.

Le projet explore l'ingestion en streaming, le traitement événementiel, la transformation des données, le masquage des données personnelles (PII) et l'orchestration de workflows à l'aide de Pub/Sub, BigQuery, Cloud Storage, Cloud Run Functions et Cloud Composer.

## Présentation du projet

Le dépôt contient quatre pipelines complémentaires :

1. ingestion native en streaming de Pub/Sub vers BigQuery ;
2. ingestion événementielle de fichiers CSV depuis Cloud Storage vers BigQuery ;
3. traitement Pub/Sub avec normalisation et masquage des adresses e-mail ;
4. orchestration quotidienne d'un processus ELT avec Cloud Composer et Apache Airflow.

## Architecture

> **Insérer ici le schéma d'architecture global du projet**

<!-- Ajouter ici l'image de l'architecture -->

## Vue d'ensemble de l'architecture

```text
Pipeline 1

Publisher
    ↓
Pub/Sub
    ↓
Abonnement BigQuery
    ↓
BigQuery orders_streaming


Pipeline 2

Fichier CSV
    ↓
Cloud Storage
    ↓
Cloud Run Function
    ↓
Pandas
    ↓
BigQuery orders_raw


Pipeline 3

Transaction Pub/Sub
    ↓
Cloud Run Function
    ↓
Validation et normalisation
    ↓
Masquage des e-mails
    ↓
BigQuery orders_streaming_safe


Pipeline 4

CSV dans Cloud Storage
    ↓
Cloud Composer / Airflow
    ↓
BigQuery orders_raw
    ↓
BigQuery orders_cleaned
    ↓
BigQuery daily_revenue
```

Un schéma d'architecture visuel sera ajouté dans cette section.

## Technologies

- Google Cloud Pub/Sub
- BigQuery
- Cloud Storage
- Cloud Run Functions
- Eventarc
- Cloud Composer
- Apache Airflow
- Python
- SQL
- Pandas
- IAM

## Structure du dépôt

```text
retail-data-pipeline-gcp/
│
├── README.md
├── .env.example
├── .gitignore
│
├── 01-pubsub-to-bigquery/
│   ├── README.md
│   ├── messages/
│   │   └── sample_events.json
│   ├── scripts/
│   │   └── publish_messages.sh
│   └── sql/
│       └── create_orders_streaming.sql
│
├── 02-event-driven-functions/
│   ├── gcs-to-bigquery/
│   │   ├── README.md
│   │   ├── main.py
│   │   ├── requirements.txt
│   │   ├── schema.json
│   │   ├── sample_data/
│   │   │   └── orders_raw.csv
│   │   └── sql/
│   │       └── create_orders_raw.sql
│   │
│   └── pubsub-to-bigquery-safe/
│       ├── README.md
│       ├── main.py
│       ├── requirements.txt
│       ├── messages/
│       │   └── sample_transaction.json
│       └── sql/
│           └── create_orders_streaming_safe.sql
│
├── 03-cloud-composer/
│   ├── README.md
│   ├── dags/
│   │   ├── daily_pipeline_step1.py
│   │   └── daily_pipeline.py
│   └── sql/
│       ├── create_orders_cleaned.sql
│       └── create_daily_revenue.sql
│
└── docs/
    ├── iam.md
    └── troubleshooting.md
```

## Pipeline 1 — Streaming natif de Pub/Sub vers BigQuery

```text
Publisher
    ↓
Topic Pub/Sub
    ↓
Abonnement BigQuery
    ↓
Table BigQuery partitionnée
```

Ce pipeline diffuse directement les événements de commandes retail de Pub/Sub vers BigQuery.

Il montre qu'un consommateur personnalisé n'est pas toujours nécessaire lorsque les messages peuvent être insérés sans transformation.

La table de destination est partitionnée à partir du timestamp de l'événement.

Plus de détails :

```text
01-pubsub-to-bigquery/README.md
```

## Pipeline 2 — De Cloud Storage vers BigQuery

```text
Fichier CSV
    ↓
Cloud Storage
    ↓
Cloud Run Function
    ↓
DataFrame Pandas
    ↓
BigQuery orders_raw
```

Le dépôt d'un fichier CSV dans Cloud Storage déclenche une Cloud Run Function.

La fonction :

- lit l'événement Cloud Storage ;
- télécharge le fichier en mémoire ;
- analyse son contenu avec Pandas ;
- convertit les valeurs manquantes ;
- insère les lignes dans BigQuery.

Cette architecture représente un modèle classique d'ingestion événementielle de fichiers provenant de fournisseurs externes.

Plus de détails :

```text
02-event-driven-functions/gcs-to-bigquery/README.md
```

## Pipeline 3 — Pub/Sub vers BigQuery avec masquage des données personnelles

```text
Message Pub/Sub
        ↓
Cloud Run Function
        ↓
Validation
        ↓
Normalisation
        ↓
Masquage des e-mails
        ↓
BigQuery
```

Ce pipeline introduit une couche de transformation entre Pub/Sub et BigQuery.

La Cloud Run Function :

- décode le payload Base64 ;
- analyse le message JSON ;
- valide les champs obligatoires ;
- normalise le pays, la catégorie et le statut ;
- convertit les champs numériques ;
- masque l'adresse e-mail du client ;
- ajoute un horodatage d'ingestion ;
- insère l'événement transformé dans BigQuery.

Exemple :

```text
customer.secret@example.com
```

devient :

```text
cu***@example.com
```

Plus de détails :

```text
02-event-driven-functions/pubsub-to-bigquery-safe/README.md
```

## Pipeline 4 — Orchestration avec Cloud Composer

```text
orders_raw.csv
      ↓
load_to_staging
      ↓
orders_raw
      ↓
transform_cleaned
      ↓
orders_cleaned
      ↓
aggregate_daily
      ↓
daily_revenue
```

Le pipeline Cloud Composer utilise Apache Airflow pour orchestrer trois tâches dépendantes :

1. charger le fichier CSV brut depuis Cloud Storage ;
2. nettoyer et standardiser les données dans BigQuery ;
3. calculer le chiffre d'affaires quotidien.

La table nettoyée est partitionnée par date et clusterisée selon des dimensions métier.

La table `daily_revenue` contient :

- le nombre de commandes ;
- les quantités vendues ;
- le chiffre d'affaires ;
- le pays ;
- la catégorie de produit.

Plus de détails :

```text
03-cloud-composer/README.md
```

## Configuration

Les exemples utilisent des identifiants Google Cloud fictifs.

Copiez le modèle d'environnement :

```bash
cp .env.example .env
```

Puis configurez :

```text
PROJECT_ID
REGION
RAW_BUCKET
BQ_DATASET
BQ_TABLE
```

Cloud Composer nécessite également les variables Airflow suivantes :

```text
project_id
raw_bucket
bq_location
```

## Données d'exemple

Toutes les données d'exemple de ce dépôt sont synthétiques.

Le fichier CSV brut contient volontairement plusieurs problèmes de qualité :

- valeurs incohérentes en majuscules et minuscules ;
- espaces en début et fin de chaîne ;
- quantités manquantes ;
- prix manquants.

Ces imperfections illustrent la différence entre :

- une couche d'ingestion brute ;
- une couche de transformation nettoyée ;
- une couche métier agrégée.

## Modèle de données

### Table brute

```text
retail.orders_raw
```

Contient les données sources sans transformation métier.

### Table nettoyée

```text
retail.orders_cleaned
```

Contient les données de commandes normalisées et validées.

### Table agrégée

```text
retail.daily_revenue
```

Contient les indicateurs métier quotidiens par pays et par catégorie.

### Table de streaming sécurisée

```text
ecom_dataset.orders_streaming_safe
```

Contient les événements de streaming normalisés avec les adresses e-mail masquées.

## Sécurité et IAM

Le projet utilise plusieurs identités Google Cloud :

- l'agent de service Pub/Sub ;
- les comptes de service d'exécution des Cloud Run Functions ;
- le compte de service Cloud Composer.

Chaque identité ne doit recevoir que les permissions nécessaires à son pipeline.

Aucun identifiant ni clé de compte de service n'est stocké dans ce dépôt.

Plus de détails :

```text
docs/iam.md
```

## Considérations pour la production

Ces implémentations ont pour objectif de démontrer les principales architectures GCP.

Une solution prête pour la production pourrait ajouter :

- des contrats de schéma explicites ;
- des topics de dead-letter ;
- une quarantaine pour les fichiers rejetés ;
- l'idempotence ;
- la détection des doublons ;
- des chargements incrémentaux ;
- des tests de qualité des données ;
- des logs structurés ;
- du monitoring et des alertes ;
- des tests unitaires et d'intégration automatisés ;
- une intégration continue ;
- des environnements séparés de développement et de production ;
- la gestion des secrets ;
- des policy tags et une sécurité au niveau des colonnes.

## Maîtrise des coûts

Les services cloud managés peuvent continuer à générer des coûts tant que les ressources restent actives.

Après les tests, les ressources inutilisées doivent être supprimées ou désactivées, notamment :

- les environnements Cloud Composer ;
- les Cloud Run Functions ;
- les déclencheurs Eventarc ;
- les topics et abonnements Pub/Sub ;
- les buckets Cloud Storage ;
- les jeux de données et tables BigQuery.

## Ce que j'ai appris

À travers ce projet, j'ai pratiqué :

- la conception de pipelines d'ingestion batch et streaming ;
- la connexion directe entre Pub/Sub et BigQuery ;
- le développement de Cloud Run Functions événementielles ;
- la lecture des événements Cloud Storage ;
- le décodage des messages Pub/Sub ;
- le traitement de fichiers CSV avec Pandas ;
- la validation et la normalisation des données d'événements ;
- le masquage des données personnelles (PII) ;
- la définition de schémas BigQuery ;
- l'utilisation du partitionnement et du clustering ;
- la création de DAGs Apache Airflow ;
- la gestion des dépendances entre tâches ;
- l'orchestration de workflows ELT avec Cloud Composer ;
- la configuration des rôles IAM et des comptes de service ;
- le diagnostic de pipelines cloud distribués.

## Contexte du projet

Ce projet a été développé dans le cadre d'un parcours d'apprentissage en Data Engineering sur Google Cloud.

Les exercices de formation d'origine ont été réorganisés et documentés sous la forme d'un projet de portfolio complet afin de présenter plusieurs modèles complémentaires de Data Engineering.

The original training exercises were reorganized and documented as a single end-to-end portfolio project to demonstrate several complementary Data Engineering patterns.
