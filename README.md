#  Banking Data Platform : Architecture Médaillon avec PySpark & Power BI

[![PySpark](https://img.shields.io/badge/PySpark-3.5+-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0+-00ADD8?logo=delta&logoColor=white)](https://delta.io/)
[![Power BI](https://img.shields.io/badge/Power_BI-Dashboard-F2C811?logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)

Plateforme de données bancaire de bout en bout implémentant une **architecture Médaillon (Bronze ➔ Silver ➔ Gold)** avec **PySpark**, **Delta Lake**, **Spark SQL** et restitution visuelle sur **Power BI**.

---

##  Architecture du Projet

Le flux de données suit les meilleures pratiques de Lakehouse :

```mermaid
flowchart LR
    A[Génération Raw / Faker] --> B[ Bronze : Ingestion Delta]
    B --> C[ Silver : Nettoyage & Typage]
    C --> D[ Gold : Agrégations Métiers Spark SQL]
    D --> E[ Power BI : Reporting & KPIs]
```

1. **Génération & Raw** : Simulation de données bancaires relationnelles (`branches`, `customers`, `accounts`, `transactions`) avec des cas réels (valeurs nulles, doublons, fraudes).
2. **Couche Bronze** : Ingestion brute et stockage au format ACID **Delta Lake**.
3. **Couche Silver** : Déduplication, validation d'intégrité, nettoyage et typage strict.
4. **Couche Gold** : Tables de faits et dimensions agrégées via **Spark SQL** (KPIs agences, Customer 360, métriques transactions, détection d'anomalies).
5. **Couche Visualisation** : Export optimisé et modélisation relationnelle dans **Power BI**.

---


##  Structure du Répertoire

```text
Projet_sparkSQL/
├── data/
│   ├── raw/                # Données brutes générées
│   ├── bronze/             # Tables Delta brutes
│   ├── silver/             # Tables Delta nettoyées
│   ├── gold/               # Tables Delta agrégées pour l'analytique
│   └── export_powerbi/     # Fichiers CSV/Parquet prêts pour Power BI
├── docs/
│   └── images/             # Captures d'écran du dashboard Power BI
│       ├── dashboard_overview.png
│       └── fraud_and_kpi.png
├── src/
│   ├── generate_data.py    # Génération synthétique avec Faker
│   ├── ingest_bronze.py    # Pipeline d'ingestion vers Bronze
│   ├── process_silver.py   # Nettoyage & contrôle qualité vers Silver
│   ├── process_gold.py     # Modélisation décisionnelle & SQL vers Gold
│   └── export_powerbi.py   # Export préparé pour Power BI
├── requirements.txt        # Dépendances Python
├── .gitignore
└── README.md
```

---

##  Installation & Prérequis

### 1. Prérequis système
* **Python** 3.10+
* **Java JDK** 8 ou 11 (requis pour Apache Spark)
* *(Sous Windows)* : Utilitaires `winutils.exe` configurés via `HADOOP_HOME`.

### 2. Installation de l'environnement
```bash
# Cloner le projet
git clone https://github.com/<votre-pseudo-github>/<nom-du-repo>.git
cd Projet_sparkSQL

# Créer et activer l'environnement virtuel
python -m venv .venv
source .venv/bin/activate    # Linux/Mac
# ou .venv\Scripts\activate   # Windows

# Installer les dépendances
pip install -r requirements.txt
```

---

##  Exécution du Pipeline de Données

Exécutez les étapes dans l'ordre suivant :

```bash
# 1. Génération des données de test
python src/generate_data.py

# 2. Ingestion dans la couche Bronze (Delta Lake)
python src/ingest_bronze.py

# 3. Traitement et nettoyage (Couche Silver)
python src/process_silver.py

# 4. Agrégations analytiques Spark SQL (Couche Gold)
python src/process_gold.py

# 5. Export des données pour Power BI
python src/export_powerbi.py
```

Une fois l'étape 5 terminée, ouvrez votre rapport **Power BI** et actualisez les sources de données situées dans `data/export_powerbi/`.

---

##  Principaux Indicateurs Analysés (KPIs)

* **Performance Agences** : Volume géré par agence, balance moyenne par compte, nombre de transactions.
* **Customer 360** : Solde total agrégé par profil client, ancienneté, nombre de comptes actifs.
* **Détection de Fraude** : Ratios de transactions atypiques, volumes anormaux et alertes de sécurité.