-- =====================================================================
-- BLOC 2 — AutoMeca Systems — Couche DATAMART (star schema)
-- PostgreSQL managé (OVHcloud, région UE)
--
-- Construite par transformation de la couche staging (Bloc 3 : pipeline
-- d'ingestion & transformation). Table de faits UNIFIÉE : les 3 sources
-- d'événements (erreurs, pannes, maintenances) partagent la même grain
-- (1 événement machine à un instant donné) et n'ont, dans les données
-- réelles, aucun attribut propre à un seul type — les regrouper évite
-- les UNION ALL répétés lors de la reconstitution de l'historique
-- chronologique d'une machine, requête centrale pour le feature
-- engineering du futur modèle prédictif.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS datamart;

-- ---------------------------------------------------------------------
-- datamart.dim_date
-- Dimension temps standard, générée (script de peuplement séparé).
-- ---------------------------------------------------------------------
CREATE TABLE datamart.dim_date (
    id_date         INT PRIMARY KEY,          -- format AAAAMMJJ, ex. 20150103
    date_complete   DATE NOT NULL UNIQUE,
    annee           SMALLINT NOT NULL,
    trimestre       SMALLINT NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    mois            SMALLINT NOT NULL CHECK (mois BETWEEN 1 AND 12),
    jour            SMALLINT NOT NULL CHECK (jour BETWEEN 1 AND 31),
    jour_semaine    VARCHAR(10) NOT NULL,
    est_weekend     BOOLEAN NOT NULL
);

COMMENT ON TABLE datamart.dim_date IS
    'Dimension temps — couvre la période du dataset (2014-06-01 à 2016-01-01)';

-- ---------------------------------------------------------------------
-- datamart.dim_machine
-- ---------------------------------------------------------------------
CREATE TABLE datamart.dim_machine (
    id_machine      SERIAL PRIMARY KEY,
    machine_id_nat  SMALLINT NOT NULL UNIQUE,   -- clé naturelle (staging.machines.machine_id)
    model           VARCHAR(20) NOT NULL,
    age             SMALLINT NOT NULL,
    tranche_age     VARCHAR(20) NOT NULL        -- ex. '0-5 ans', '6-10 ans', ...
);

COMMENT ON TABLE datamart.dim_machine IS
    'Dimension machine — 100 machines, model1-4, âge 0-20 ans';

-- ---------------------------------------------------------------------
-- datamart.dim_type_evenement
-- Petite dimension de référence : les 3 natures d'événement.
-- ---------------------------------------------------------------------
CREATE TABLE datamart.dim_type_evenement (
    id_type_evenement   SMALLSERIAL PRIMARY KEY,
    code_type           VARCHAR(20) NOT NULL UNIQUE,   -- 'ERREUR' | 'PANNE' | 'MAINTENANCE'
    libelle             VARCHAR(60) NOT NULL
);

INSERT INTO datamart.dim_type_evenement (code_type, libelle) VALUES
    ('ERREUR',      'Alerte machine (log erreur)'),
    ('PANNE',       'Panne non planifiée (remplacement composant)'),
    ('MAINTENANCE', 'Maintenance planifiée (remplacement préventif)');

-- ---------------------------------------------------------------------
-- datamart.dim_code_evenement
-- Dimension pont regroupant les deux référentiels de code observés
-- dans les données (error1-5 pour les erreurs, comp1-4 pour les
-- pannes/maintenances) sous une clé de substitution commune.
-- ---------------------------------------------------------------------
CREATE TABLE datamart.dim_code_evenement (
    id_code             SERIAL PRIMARY KEY,
    id_type_evenement   SMALLINT NOT NULL REFERENCES datamart.dim_type_evenement(id_type_evenement),
    code_brut           VARCHAR(10) NOT NULL,   -- 'error1'..'error5' ou 'comp1'..'comp4'
    libelle             VARCHAR(60),
    UNIQUE (id_type_evenement, code_brut)
);

COMMENT ON TABLE datamart.dim_code_evenement IS
    'Codes d''événement — error1-5 (type ERREUR) et comp1-4 (types PANNE et MAINTENANCE)';

-- ---------------------------------------------------------------------
-- datamart.dim_operateur
-- Opérateur/technicien ayant réalisé une intervention (panne ou
-- maintenance — une simple alerte n'implique aucune intervention
-- humaine). ANTICIPÉE POUR UNE FUTURE INTÉGRATION GMAO : le dataset
-- source (Kaggle Microsoft Azure Predictive Maintenance) ne contient
-- aucune donnée opérateur. Table modélisée mais non peuplée dans
-- cette itération — voir dictionnaire_donnees.md.
-- ---------------------------------------------------------------------
CREATE TABLE datamart.dim_operateur (
    id_operateur    SERIAL PRIMARY KEY,
    matricule       VARCHAR(20),
    nom             VARCHAR(100),
    equipe          VARCHAR(50)
);

COMMENT ON TABLE datamart.dim_operateur IS
    'Opérateur ayant réalisé une intervention — non peuplée, absente du dataset source, anticipe une intégration GMAO future';

-- ---------------------------------------------------------------------
-- datamart.fait_evenement
-- Table de faits unifiée. Grain : 1 ligne = 1 événement machine
-- (erreur, panne ou maintenance) à un instant donné.
-- ---------------------------------------------------------------------
CREATE TABLE datamart.fait_evenement (
    id_evenement       BIGSERIAL PRIMARY KEY,
    id_date            INT NOT NULL REFERENCES datamart.dim_date(id_date),
    datetime_evt        TIMESTAMP NOT NULL,   -- horodatage précis (dim_date = granularité jour)
    id_machine         INT NOT NULL REFERENCES datamart.dim_machine(id_machine),
    id_type_evenement  SMALLINT NOT NULL REFERENCES datamart.dim_type_evenement(id_type_evenement),
    id_code            INT NOT NULL REFERENCES datamart.dim_code_evenement(id_code),
    id_operateur        INT REFERENCES datamart.dim_operateur(id_operateur),  -- NULL pour ERREUR (pas d'intervention) ; non renseigné pour PANNE/MAINTENANCE (absent du dataset source)
    source_fichier      VARCHAR(40) NOT NULL   -- traçabilité : PdM_errors.csv / PdM_failures.csv / PdM_maint.csv
);

CREATE INDEX idx_fait_evenement_machine_date ON datamart.fait_evenement (id_machine, datetime_evt);
CREATE INDEX idx_fait_evenement_type ON datamart.fait_evenement (id_type_evenement);

COMMENT ON TABLE datamart.fait_evenement IS
    'Fait unifié erreurs/pannes/maintenances — grain 1 ligne = 1 événement machine horodaté';

-- ---------------------------------------------------------------------
-- datamart.v_interventions
-- Vue "interventions" au sens strict (pannes + maintenances) sur le
-- fait unifié : une alerte seule (ERREUR) n'est pas une intervention.
-- Répond au besoin de reporting maintenance sans dupliquer le fait.
-- ---------------------------------------------------------------------
CREATE VIEW datamart.v_interventions AS
SELECT f.*
FROM datamart.fait_evenement f
JOIN datamart.dim_type_evenement t ON t.id_type_evenement = f.id_type_evenement
WHERE t.code_type IN ('PANNE', 'MAINTENANCE');

COMMENT ON VIEW datamart.v_interventions IS
    'Sous-ensemble du fait unifié restreint aux interventions réelles (pannes + maintenances), pour le reporting maintenance';
