-- =====================================================================
-- Bloc 3 — Nouvelle table de faits : consolidation quotidienne
--
-- Ne modifie ni ne remplace le DDL du Bloc 2 (02_datamart.sql) : ajoute
-- une table de faits supplementaire au schema en etoile existant, a un
-- grain different de datamart.fait_evenement (1 ligne = 1 evenement)
-- -- ici, 1 ligne = 1 machine x 1 jour. Partage les memes dimensions
-- (dim_machine, dim_date) : modelisation Kimball standard, plusieurs
-- faits a des grains differents sur des dimensions conformes.
--
-- Alimentee par transform/consolidation.py, qui joint les agregats
-- quotidiens de telemetrie (ClickHouse, vue automeca.v_telemetrie_
-- tendance) avec les evenements du jour (datamart.fait_evenement) —
-- jointure croisee-moteur faite en Python, aucun des deux moteurs ne
-- sachant nativement interroger l'autre sans connecteur externe.
--
-- A executer UNE SEULE FOIS, apres le DDL du Bloc 2 et
-- 00_add_missing_constraints.sql, avant la premiere execution de
-- consolidation.py — meme logique que les prerequis precedents.
-- =====================================================================

CREATE TABLE datamart.fait_telemetrie_jour (
    id_fait_telemetrie_jour    BIGSERIAL PRIMARY KEY,
    id_date                    INT NOT NULL REFERENCES datamart.dim_date(id_date),
    id_machine                 INT NOT NULL REFERENCES datamart.dim_machine(id_machine),
    rms_vibration_moyen        DOUBLE PRECISION,   -- NULL si aucune telemetrie ce jour-la
    rms_vibration_max          DOUBLE PRECISION,
    tendance_pression_moyenne  DOUBLE PRECISION,
    nb_erreurs                 SMALLINT NOT NULL DEFAULT 0,
    nb_pannes                  SMALLINT NOT NULL DEFAULT 0,
    nb_maintenances             SMALLINT NOT NULL DEFAULT 0,
    UNIQUE (id_date, id_machine)
);

CREATE INDEX idx_fait_telemetrie_jour_machine ON datamart.fait_telemetrie_jour (id_machine, id_date);

COMMENT ON TABLE datamart.fait_telemetrie_jour IS
    'Consolidation quotidienne machine x jour — agregats telemetrie ClickHouse + compteurs evenements datamart, alimente le reentrainement periodique du modele predictif (Bloc 4)';
