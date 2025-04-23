# 4DATA-projet

# Pipeline de Données Bison Futé avec Dagster

Ce projet implémente une pipeline ETL complète permettant d'extraire, transformer et analyser les données d'événements routiers depuis l'API Bison Futé.

## Fonctionnalités

- Extraction automatique des fichiers XML d'événements routiers depuis l'API Bison Futé
- Parsing et transformation des données XML en format structuré
- Nettoyage et normalisation des données
- Stockage dans MongoDB pour persistance
- Visualisations et tableau de bord pour l'analyse des données
- Planification automatique des actualisations de données
- Détection de nouveaux fichiers XML

## Architecture

Le projet suit une architecture ETL classique:

1. **Extraction** - Récupération des fichiers XML depuis l'API Bison Futé
2. **Transformation** - Parsing XML, normalisation et nettoyage des données
3. **Chargement** - Stockage dans MongoDB et export CSV
4. **Visualisation** - Génération de graphiques et d'un tableau de bord

## Technologies utilisées

- **Dagster** - Orchestration de la pipeline de données
- **MongoDB** - Stockage persistant des données
- **pandas** - Manipulation et transformation des données
- **lxml** - Parsing des fichiers XML
- **Matplotlib/Seaborn** - Visualisation des données
- **Docker** - Conteneurisation de l'application

## Installation

### Prérequis
- Docker et Docker Compose
- Git

### Déploiement
1. Cloner le dépôt
   ```bash
   git clone https://github.com/luluscript/4DATA-projet.git

2. Lancer les conteneurs Docker

   ```bash
    task reset-no-cache

3. Accéder à l'interface Dagster

http://localhost:3000

### Utilisation
Matérialisation manuelle des assets
- Accéder à l'interface Dagster
- Aller dans la section "Assets"
- Cliquer sur "Materialize all"

### Visualisations
Les visualisations sont générées dans le dossier data/visualisations et peuvent être consultées directement ou via le tableau de bord généré à l'adresse data/dashboard.html.

### Architecture technique
#### Extraction des données
Source : API Bison Futé (tipi.bison-fute.gouv.fr)
Données : Fichiers XML DATEX II contenant des informations sur le trafic routier
#### Stockage
MongoDB pour le stockage des données transformées
#### Transformation
Parsing XML avec lxml
Nettoyage et standardisation des données avec pandas
#### Monitoring
Logs détaillés accessibles via l'interface Dagster

#### Choix de conception
Dagster : Choisi pour sa flexibilité et son approche data-centric avec le concept d'assets
MongoDB : Base NoSQL adaptée aux données semi-structurées variables
Docker : Garantit la portabilité et la reproductibilité de l'environnement

### La schedule (bison_fute_daily_schedule)
La schedule est configurée pour exécuter automatiquement le job mon_job tous les jours à 6h00 du matin. En pratique, cela signifie que:

Chaque jour à 6h00 du matin, Dagster démarrera automatiquement le pipeline de traitement

Ce pipeline exécutera séquentiellement toutes les étapes définies dans le job:
- Récupération des fichiers XML
- Analyse (parsing) de ces fichiers
- Nettoyage des données extraites
- Export des données en CSV
- Chargement des données dans MongoDB
Cette automatisation garantit que le traitement s'exécute quotidiennement sans intervention manuelle.

### Le sensor (bison_fute_file_sensor)
Le sensor surveille en continu l'arrivée de nouveaux fichiers XML dans un répertoire spécifié. Son fonctionnement est le suivant:

Le sensor s'active périodiquement (typiquement toutes les 30 secondes par défaut)
À chaque activation, il vérifie si de nouveaux fichiers XML sont apparus dans le répertoire surveillé
S'il détecte de nouveaux fichiers, il:
- Enregistre ces fichiers comme "déjà traités" pour ne pas les retraiter à l'avenir
- Déclenche immédiatement une exécution du job pour traiter ces nouveaux fichiers
- Si aucun nouveau fichier n'est détecté, il ne fait rien et attendra la prochaine vérification