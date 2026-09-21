-- =====================================================================
-- Bloc 3 — Contraintes d'unicite complementaires au DDL du Bloc 2
--
-- Ne modifie ni ne remplace 01_staging.sql / 02_datamart.sql du depot
-- Bloc 2 : ajoute seulement les contraintes d'unicite naturelle qui y
-- manquaient, necessaires pour que ON CONFLICT DO NOTHING rende le
-- pipeline idempotent (rejouable sans creer de doublons).
--
-- A executer UNE SEULE FOIS, apres le DDL du Bloc 2 et avant la
-- premiere execution du pipeline — meme logique que le DDL lui-meme
-- (prerequis d'installation, pas une etape du pipeline recurrent).
-- =====================================================================

ALTER TABLE staging.erreurs
    ADD CONSTRAINT uq_erreurs_natural UNIQUE (datetime_evt, machine_id, error_id);

ALTER TABLE staging.pannes
    ADD CONSTRAINT uq_pannes_natural UNIQUE (datetime_evt, machine_id, failure_comp);

ALTER TABLE staging.maintenances
    ADD CONSTRAINT uq_maintenances_natural UNIQUE (datetime_evt, machine_id, comp);

-- fait_evenement (datamart) : meme besoin, decouvert lors de l'ecriture
-- de la transformation staging -> datamart (sql/transform_datamart.sql).
ALTER TABLE datamart.fait_evenement
    ADD CONSTRAINT uq_fait_evenement_natural UNIQUE (id_machine, datetime_evt, id_type_evenement, id_code);
