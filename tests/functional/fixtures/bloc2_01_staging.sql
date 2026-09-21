-- =====================================================================
-- BLOC 2 — AutoMeca Systems — Couche STAGING (modèle entité-relation)
-- PostgreSQL managé (OVHcloud, région UE)
--
-- Reflète fidèlement les 5 fichiers sources du dataset Microsoft Azure
-- Predictive Maintenance, tels que déposés dans Object Storage.
-- La télémétrie (PdM_telemetry.csv) n'a pas de table de staging ici :
-- son volume (876 100 lignes/an) et sa nature série temporelle la
-- destinent directement à ClickHouse (voir 03_telemetrie_clickhouse.sql).
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS staging;

-- ---------------------------------------------------------------------
-- staging.machines  (source : PdM_machines.csv)
-- Référentiel des machines. Grain : 1 ligne par machine.
-- ---------------------------------------------------------------------
CREATE TABLE staging.machines (
    machine_id      SMALLINT PRIMARY KEY,
    model           VARCHAR(20) NOT NULL,
    age             SMALLINT NOT NULL CHECK (age BETWEEN 0 AND 40)
);

COMMENT ON TABLE staging.machines IS
    'Référentiel machines — source PdM_machines.csv (100 machines, model1-4, âge 0-20 ans)';

-- ---------------------------------------------------------------------
-- staging.erreurs  (source : PdM_errors.csv)
-- Alertes/erreurs machine. Grain : 1 ligne par événement d'erreur.
-- ---------------------------------------------------------------------
CREATE TABLE staging.erreurs (
    id_erreur       BIGSERIAL PRIMARY KEY,
    datetime_evt    TIMESTAMP NOT NULL,
    machine_id      SMALLINT NOT NULL REFERENCES staging.machines(machine_id),
    error_id        VARCHAR(10) NOT NULL   -- error1 .. error5
);

CREATE INDEX idx_erreurs_machine_date ON staging.erreurs (machine_id, datetime_evt);

COMMENT ON TABLE staging.erreurs IS
    'Alertes machine — source PdM_errors.csv (~3 900 évts, error1-5, 2015-01-03 à 2016-01-01)';

-- ---------------------------------------------------------------------
-- staging.pannes  (source : PdM_failures.csv)
-- Pannes non planifiées avec remplacement de composant.
-- Grain : 1 ligne par panne. C'est la table qui portera le label
-- (cible) du futur modèle prédictif (Bloc 3/4).
-- ---------------------------------------------------------------------
CREATE TABLE staging.pannes (
    id_panne        BIGSERIAL PRIMARY KEY,
    datetime_evt    TIMESTAMP NOT NULL,
    machine_id      SMALLINT NOT NULL REFERENCES staging.machines(machine_id),
    failure_comp    VARCHAR(10) NOT NULL   -- comp1 .. comp4
);

CREATE INDEX idx_pannes_machine_date ON staging.pannes (machine_id, datetime_evt);

COMMENT ON TABLE staging.pannes IS
    'Pannes non planifiées — source PdM_failures.csv (~760 évts, comp1-4, 2015-01-05 à 2015-12-31)';

-- ---------------------------------------------------------------------
-- staging.maintenances  (source : PdM_maint.csv)
-- Maintenances planifiées / remplacements préventifs de composant.
-- Grain : 1 ligne par intervention de maintenance.
-- ---------------------------------------------------------------------
CREATE TABLE staging.maintenances (
    id_maintenance  BIGSERIAL PRIMARY KEY,
    datetime_evt    TIMESTAMP NOT NULL,
    machine_id      SMALLINT NOT NULL REFERENCES staging.machines(machine_id),
    comp            VARCHAR(10) NOT NULL   -- comp1 .. comp4
);

CREATE INDEX idx_maintenances_machine_date ON staging.maintenances (machine_id, datetime_evt);

COMMENT ON TABLE staging.maintenances IS
    'Maintenances planifiées — source PdM_maint.csv (~3 300 évts, comp1-4, 2014-06-01 à 2016-01-01)';
