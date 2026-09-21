-- =====================================================================
-- Bloc 3 — Transform : staging -> datamart (fusion unifiee)
--
-- Execute par l'orchestrateur (Airflow) apres load_staging.py. Toutes
-- les etapes sont idempotentes (ON CONFLICT DO NOTHING / UPSERT) grace
-- aux contraintes ajoutees dans 00_add_missing_constraints.sql : une
-- reexecution ne cree jamais de doublon.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1) dim_machine — synchronisation depuis staging.machines
-- ---------------------------------------------------------------------
INSERT INTO datamart.dim_machine (machine_id_nat, model, age, tranche_age)
SELECT
    m.machine_id,
    m.model,
    m.age,
    CASE
        WHEN m.age <= 5  THEN '0-5 ans'
        WHEN m.age <= 10 THEN '6-10 ans'
        WHEN m.age <= 15 THEN '11-15 ans'
        ELSE '16-20 ans'
    END
FROM staging.machines m
ON CONFLICT (machine_id_nat) DO UPDATE
    SET model = EXCLUDED.model,
        age = EXCLUDED.age,
        tranche_age = EXCLUDED.tranche_age;

-- ---------------------------------------------------------------------
-- 2) dim_date — une ligne par jour distinct present dans les sources
-- ---------------------------------------------------------------------
INSERT INTO datamart.dim_date (id_date, date_complete, annee, trimestre, mois, jour, jour_semaine, est_weekend)
SELECT DISTINCT
    (to_char(d, 'YYYYMMDD'))::int,
    d,
    extract(year FROM d)::smallint,
    extract(quarter FROM d)::smallint,
    extract(month FROM d)::smallint,
    extract(day FROM d)::smallint,
    -- Mapping manuel (independant de la locale du serveur PostgreSQL,
    -- qui renverrait "Monday" en anglais avec to_char(d,'FMDay') sur
    -- un serveur configure par defaut).
    CASE extract(isodow FROM d)
        WHEN 1 THEN 'lundi' WHEN 2 THEN 'mardi' WHEN 3 THEN 'mercredi'
        WHEN 4 THEN 'jeudi' WHEN 5 THEN 'vendredi' WHEN 6 THEN 'samedi'
        ELSE 'dimanche'
    END,
    extract(isodow FROM d) IN (6, 7)
FROM (
    SELECT datetime_evt::date AS d FROM staging.erreurs
    UNION
    SELECT datetime_evt::date FROM staging.pannes
    UNION
    SELECT datetime_evt::date FROM staging.maintenances
) dates
ON CONFLICT (id_date) DO NOTHING;

-- ---------------------------------------------------------------------
-- 3) dim_code_evenement — codes error1-5 (ERREUR) et comp1-4 (PANNE, MAINTENANCE)
-- ---------------------------------------------------------------------
INSERT INTO datamart.dim_code_evenement (id_type_evenement, code_brut, libelle)
SELECT DISTINCT t.id_type_evenement, e.error_id, e.error_id
FROM staging.erreurs e
JOIN datamart.dim_type_evenement t ON t.code_type = 'ERREUR'
ON CONFLICT (id_type_evenement, code_brut) DO NOTHING;

INSERT INTO datamart.dim_code_evenement (id_type_evenement, code_brut, libelle)
SELECT DISTINCT t.id_type_evenement, p.failure_comp, p.failure_comp
FROM staging.pannes p
JOIN datamart.dim_type_evenement t ON t.code_type = 'PANNE'
ON CONFLICT (id_type_evenement, code_brut) DO NOTHING;

INSERT INTO datamart.dim_code_evenement (id_type_evenement, code_brut, libelle)
SELECT DISTINCT t.id_type_evenement, mt.comp, mt.comp
FROM staging.maintenances mt
JOIN datamart.dim_type_evenement t ON t.code_type = 'MAINTENANCE'
ON CONFLICT (id_type_evenement, code_brut) DO NOTHING;

-- ---------------------------------------------------------------------
-- 4) fait_evenement — fusion des 3 sources (id_operateur toujours NULL :
--    absent du dataset source, voir Bloc 2 / dim_operateur)
-- ---------------------------------------------------------------------
INSERT INTO datamart.fait_evenement (id_date, datetime_evt, id_machine, id_type_evenement, id_code, id_operateur, source_fichier)
SELECT
    (to_char(e.datetime_evt, 'YYYYMMDD'))::int,
    e.datetime_evt,
    dm.id_machine,
    dc.id_type_evenement,
    dc.id_code,
    NULL,
    'PdM_errors.csv'
FROM staging.erreurs e
JOIN datamart.dim_machine dm ON dm.machine_id_nat = e.machine_id
JOIN datamart.dim_type_evenement dt ON dt.code_type = 'ERREUR'
JOIN datamart.dim_code_evenement dc ON dc.id_type_evenement = dt.id_type_evenement AND dc.code_brut = e.error_id
ON CONFLICT ON CONSTRAINT uq_fait_evenement_natural DO NOTHING;

INSERT INTO datamart.fait_evenement (id_date, datetime_evt, id_machine, id_type_evenement, id_code, id_operateur, source_fichier)
SELECT
    (to_char(p.datetime_evt, 'YYYYMMDD'))::int,
    p.datetime_evt,
    dm.id_machine,
    dc.id_type_evenement,
    dc.id_code,
    NULL,
    'PdM_failures.csv'
FROM staging.pannes p
JOIN datamart.dim_machine dm ON dm.machine_id_nat = p.machine_id
JOIN datamart.dim_type_evenement dt ON dt.code_type = 'PANNE'
JOIN datamart.dim_code_evenement dc ON dc.id_type_evenement = dt.id_type_evenement AND dc.code_brut = p.failure_comp
ON CONFLICT ON CONSTRAINT uq_fait_evenement_natural DO NOTHING;

INSERT INTO datamart.fait_evenement (id_date, datetime_evt, id_machine, id_type_evenement, id_code, id_operateur, source_fichier)
SELECT
    (to_char(mt.datetime_evt, 'YYYYMMDD'))::int,
    mt.datetime_evt,
    dm.id_machine,
    dc.id_type_evenement,
    dc.id_code,
    NULL,
    'PdM_maint.csv'
FROM staging.maintenances mt
JOIN datamart.dim_machine dm ON dm.machine_id_nat = mt.machine_id
JOIN datamart.dim_type_evenement dt ON dt.code_type = 'MAINTENANCE'
JOIN datamart.dim_code_evenement dc ON dc.id_type_evenement = dt.id_type_evenement AND dc.code_brut = mt.comp
ON CONFLICT ON CONSTRAINT uq_fait_evenement_natural DO NOTHING;
