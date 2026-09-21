# AutoMeca Systems — Maintenance prédictive IoT

![Kafka](https://img.shields.io/badge/ingestion-Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![ClickHouse](https://img.shields.io/badge/t%C3%A9l%C3%A9metrie-ClickHouse-FFCC01?style=flat-square&logo=clickhouse&logoColor=black)
![PostgreSQL](https://img.shields.io/badge/datamart-PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/extraction-Python-3776AB?style=flat-square&logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/orchestration-Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)

AutoMeca Systems conçoit des équipements de freinage pour l'industrie
automobile. Ce projet met en place une plateforme de données pour la
maintenance prédictive de son parc de machines de production : capteurs
IoT en atelier, modélisation et stockage des données, pipelines
d'ingestion, et déploiement d'un modèle prédictif de panne.

Le projet est organisé en dépôts indépendants, un par domaine :

| Dépôt | Contenu |
|---|---|
| [Bloc2-AutoMeca-Maintenance-Predictive-IoT-Architecture-Data](https://github.com/<user>/Bloc2-AutoMeca-Maintenance-Predictive-IoT-Architecture-Data) | Architecture de données : diagramme Edge/Cloud, modèle conceptuel, star schema, dictionnaire de données |
| [Bloc3-AutoMeca-Maintenance-Predictive-IoT-Pipeline-Data](https://github.com/<user>/Bloc3-AutoMeca-Maintenance-Predictive-IoT-Pipeline-Data) | Pipelines d'ingestion et de transformation des données (ELT) |
| [bloc4-solution-ia](https://github.com/<user>/bloc4-solution-ia) | Modèle de maintenance prédictive et déploiement |
| [bloc4-cicd](https://github.com/<user>/bloc4-cicd) | Intégration et déploiement continus |

---

## Ce dépôt : Bloc3-AutoMeca-Maintenance-Predictive-IoT-Pipeline-Data

Pipeline de données en approche **ELT** (Extract, Load, Transform) —
un seul modèle, décliné à deux cadences :

- **Flux temps réel** (capteurs critiques, continu) : ingestion via
  MQTT/Kafka, dépôt brut dans Object Storage (data lake, cohérent avec
  les 4 autres sources du Bloc 2) puis chargement dans ClickHouse ; une
  **vue matérialisée**, recalculée automatiquement à chaque insertion
  (fenêtre tumbling d'1 minute par machine), produit le RMS vibratoire
  et la tendance de pression — une alerte précoce est ensuite déclenchée
  si un seuil est dépassé (règle Grafana, voir `dashboard/`).
- **Flux batch quotidien** : extraction GMAO/ERP + capteurs, chargement
  brut (Object Storage → staging), puis transformation SQL vers le
  star schema du Bloc 2. Une étape de **consolidation** joint ensuite
  ce datamart avec des agrégats quotidiens de télémétrie (issus de la
  vue matérialisée ClickHouse), avant d'alimenter le réentraînement
  périodique du modèle prédictif (Bloc 4).

Les deux flux suivent strictement le même schéma **Extract → Load →
Transform** : la donnée brute est toujours chargée en premier, la
transformation a toujours lieu après coup, en SQL, dans la base de
destination — jamais en amont du chargement.

Toutes les sources convergent vers un **data lake unique et centralisé**
(Object Storage OVHcloud, partagé entre les deux flux) — la télémétrie
y est déposée au format **Parquet**, adapté aux séries temporelles.

> [!NOTE]
> Le pipeline ne crée jamais les tables de destination (staging, datamart, télémétrie) — elles sont créées séparément via les scripts du dépôt [Bloc2-AutoMeca-Maintenance-Predictive-IoT-Architecture-Data](https://github.com/<user>/Bloc2-AutoMeca-Maintenance-Predictive-IoT-Architecture-Data), en amont, comme prérequis documenté (pas de dépendance technique entre les deux dépôts). **Prérequis complémentaires, propres à ce dépôt**, à exécuter une seule fois juste après le DDL du Bloc 2 : `transform/00_add_missing_constraints.sql` (contraintes d'unicité manquantes, nécessaires pour que le pipeline soit réellement idempotent — `ON CONFLICT DO NOTHING`), `transform/01_add_fait_telemetrie_jour.sql` (nouvelle table de faits pour la consolidation quotidienne), `quality/00_add_quarantine_table.sql` (table ClickHouse de quarantaine) et `privacy/00_widen_matricule_column.sql` (colonne élargie pour stocker un pseudonyme complet).

> [!IMPORTANT]
> La vue matérialisée ClickHouse se recalcule automatiquement à chaque insertion — RMS et tendance restent à jour en quelques secondes, **sans moteur de traitement de flux externe** (pas de cluster Spark Streaming à opérer). Le calcul temps réel reste ainsi entièrement en SQL, cohérent avec le reste du pipeline ELT.

> [!NOTE]
> Une source **thermographies** (imagerie thermique) est anticipée dans la conception du pipeline mais n'est pas disponible dans le dataset source actuel (Kaggle) — même traitement que la dimension `opérateur` au Bloc 2 : prévue, non peuplée pour cette itération.

> [!IMPORTANT]
> Le pipeline est orchestré de bout en bout par **Airflow**, sans intervention manuelle. Chaque tâche dispose d'une **reprise automatique sur erreur** (relance idempotente) et déclenche une **alerte opérationnelle immédiate** en cas d'échec — distincte de l'alerte précoce métier (qui signale une panne probable, pas un problème du pipeline).

> [!NOTE]
> Le volet RGPD du diagramme (page 2) a deux parties distinctes, reliées par un trait en pointillé (pas une étape du flux de données) : la **pseudonymisation** (`privacy/pseudonymisation.py`, code applicatif, prêt) et le **journal d'accès & traitements** (traçabilité des accès humains à `dim_operateur`) — ce second volet est une configuration d'infrastructure (extension `pgaudit` sur PostgreSQL), pas du code, et sera activée **au moment du provisionnement OVHcloud**, pas avant.

Une seconde page du diagramme couvre les contrôles transverses au
pipeline : **validation & quarantaine** des données capteurs
(valeurs aberrantes, données manquantes — isolées avant toute
intégration), un **tableau de bord** de supervision (fraîcheur des
données, taux d'erreur, latence bout-en-bout), et le volet **RGPD**
(pseudonymisation des données croisées avec les opérateurs dès
l'ingestion, journal d'accès et de traitements pour la traçabilité).

## 🗂️ Structure

```
Bloc3-AutoMeca-Maintenance-Predictive-IoT-Pipeline-Data/
├── diagram/
│   ├── pipeline-elt-bloc3.pptx        # source éditable, 2 pages
│   ├── pipeline-elt-bloc3.pdf         # export portable, 2 pages
│   ├── 01_pipeline-elt-bloc3.png      # page 1 : flux temps réel + flux batch (ELT)
│   └── 02_qualite-monitoring-rgpd.png # page 2 : validation, dashboard, RGPD
├── common/                # config (env), logs, client Object Storage partagé
├── extraction/             # flux batch — Extract + Load
│   ├── extract_kaggle.py       # Kaggle API -> data lake (GMAO/ERP)
│   └── load_staging.py         # data lake -> staging PostgreSQL
├── ext_load_streaming/      # flux temps réel — Extract + Load
│   ├── mqtt_to_kafka.py         # capteurs MQTT -> Kafka (Extract)
│   └── kafka_to_clickhouse.py   # Kafka -> data lake (Parquet) + ClickHouse (Load, par lots)
├── transform/               # Transform — requêtes réelles
│   ├── 00_add_missing_constraints.sql  # prérequis, complément au DDL Bloc 2
│   ├── transform_datamart.sql          # fusion staging -> datamart (testée, idempotente)
│   ├── materialized_view_telemetrie.sql  # RMS + tendance par fenêtre 1 min (testée sur ClickHouse réel)
│   ├── 01_add_fait_telemetrie_jour.sql   # nouvelle table de faits (grain machine x jour), prérequis
│   └── consolidation.py                  # jointure télémétrie ClickHouse + datamart PostgreSQL (testée de bout en bout)
├── quality/                 # validation + quarantaine
│   ├── validation_rules.py     # regle par mesure (hors plage) + regle periodique (capteur silencieux)
│   └── 00_add_quarantine_table.sql  # table ClickHouse dediee, prerequis
├── privacy/                 # RGPD — pseudonymisation
│   ├── pseudonymisation.py     # HMAC-SHA256 a cle, deterministe, non reversible sans la cle
│   └── 00_widen_matricule_column.sql  # dim_operateur.matricule -> VARCHAR(64), prerequis
├── orchestration/            # DAG Airflow (a venir)
├── dashboard/                 # config Grafana (a venir)
├── tests/
│   ├── unit/                  # 43 tests, mocks — aucune connexion reelle
│   └── functional/            # (a venir)
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

## 🛠️ Stack technique

- 📡 **Kafka managé (OVHcloud)** — broker d'ingestion (MQTT → Kafka)
- ⚡ **ClickHouse** — stockage de la télémétrie brute (Parquet) + vue matérialisée (RMS, tendance, alerting)
- 🐘 **PostgreSQL** — staging + datamart (Bloc 2)
- ☁️ **Object Storage (OVHcloud)** — data lake centralisé unique, partagé entre les deux flux
- 🪁 **Airflow** — orchestration de bout en bout, reprise sur erreur, alerting opérationnel

## 📦 Contenu

- **`diagram/`** — page 1 : le pipeline ELT unique décliné à deux cadences (flux temps réel capteurs → Kafka → ClickHouse → vue matérialisée → alerte précoce ; flux batch GMAO/ERP → Object Storage → staging → datamart → consolidation) ; page 2 : validation/quarantaine des données, tableau de bord de supervision, pseudonymisation RGPD et traçabilité
- **`common/`** — utilitaires partagés par tout le pipeline : configuration (secrets via variables d'environnement, jamais en dur), logs structurés, client Object Storage
- **`extraction/`** — flux batch : récupération des 4 fichiers GMAO/ERP (Kaggle API) vers le data lake, puis chargement brut vers le staging PostgreSQL — idempotent
- **`ext_load_streaming/`** — flux temps réel : pont MQTT → Kafka (structurel, aucune validation métier), puis consommateur Kafka qui applique les règles de `quality/` avant de charger par lots la télémétrie valide dans Object Storage (Parquet) et la table ClickHouse `automeca.telemetrie` — offset Kafka commité seulement après succès des écritures
- **`transform/`** — le prérequis de contraintes (complément au DDL du Bloc 2), la transformation staging → datamart (testée contre un vrai PostgreSQL, fusion correcte et idempotence confirmées), la vue matérialisée ClickHouse RMS vibratoire + tendance de pression par fenêtre d'1 minute (testée contre un vrai ClickHouse), et la consolidation quotidienne — nouvelle table de faits `datamart.fait_telemetrie_jour` (grain machine × jour, dimensions `dim_machine`/`dim_date` réutilisées) alimentée par un script Python qui joint ClickHouse et PostgreSQL (jointure cross-moteur impossible nativement en SQL sans connecteur fragile) ; testée de bout en bout contre un vrai PostgreSQL + ClickHouse : valeurs recalculées vérifiées manuellement, idempotence confirmée sur une deuxième exécution
- **`quality/`** — deux contrôles de nature différente (voir page 2 du diagramme) : **valeur hors plage** (par mesure — bornes calculées sur les 876 100 mesures réelles du dataset Kaggle, marge de sécurité pour ne jamais rejeter une vraie valeur historique ; mesure suspecte isolée dans `automeca.telemetrie_quarantaine`, jamais fusionnée) et **capteur silencieux** (contrôle périodique — pas de message à mettre en quarantaine quand aucune donnée n'arrive ; fonction prête, câblée plus tard par `orchestration/`) ; testé de bout en bout contre un vrai ClickHouse — a aussi révélé et corrigé un bug de typage (`datetime_mes` inséré comme chaîne au lieu d'un vrai `datetime`, jamais détecté par les tests mockés)
- **`privacy/`** — pseudonymisation des identités opérateur par HMAC-SHA256 à clé (déterministe, non réversible sans la clé secrète — `PRIVACY_PSEUDONYMISATION_KEY`) ; prête mais pas encore câblée dans un flux actif, `dim_operateur` (Bloc 2) n'étant pas peuplée. Le journal d'accès & traitements (RGPD, 2ᵉ volet) est une configuration d'infrastructure (`pgaudit`), pas du code — voir la note plus haut
- **`tests/unit/`** — 43 tests couvrant l'extraction, le chargement staging, le pipeline temps réel, la consolidation, la qualité des données et la pseudonymisation, sans connexion réelle (mocks)

Composants restants (privacy, orchestration, dashboard) : à venir.
