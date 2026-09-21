-- =====================================================================
-- Bloc 3 — Transform : RMS vibratoire + tendance de pression (ClickHouse)
--
-- Une vue materialisee ClickHouse ne s'execute que sur le lot de lignes
-- qui vient d'etre insere (pas d'acces a l'historique deja stocke) : une
-- fenetre glissante continue n'est donc pas incrementale nativement en
-- SQL pur. Le mecanisme retenu ici est une fenetre tumbling courte (1
-- minute, par machine), qui se raffine automatiquement a chaque insert
-- grace au moteur AggregatingMergeTree (fusion des etats partiels) —
-- c'est ce qui tient la promesse "sans moteur de traitement de flux
-- externe" documentee dans le README.
--
-- Ne declenche pas l'alerte elle-meme : les seuils (ALERT_RMS_VIBRATION_
-- THRESHOLD, ALERT_PRESSURE_TREND_THRESHOLD, voir common/config.py)
-- seront exploites par une regle d'alerte Grafana (dashboard/, a venir),
-- pas par du code Python redondant.
--
-- Prerequis : la base et la table brute automeca.telemetrie existent
-- deja (Bloc 2, database_scripts/03_telemetrie_clickhouse.sql).
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1) Table d'etat — un etat partiel par machine x fenetre de 1 minute
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS automeca.telemetrie_fenetre_1min
(
    machine_id UInt16,
    fenetre DateTime,
    rms_vibration_state AggregateFunction(avg, Float64),
    pression_state AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(fenetre)
ORDER BY (machine_id, fenetre);

-- ---------------------------------------------------------------------
-- 2) Vue materialisee — alimente la table d'etat a chaque insertion
--    dans automeca.telemetrie (le RMS se calcule via sqrt(avg(x^2)),
--    donc l'etat partiel stocke est avgState(vibration^2))
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS automeca.mv_telemetrie_fenetre_1min
TO automeca.telemetrie_fenetre_1min
AS
SELECT
    machine_id,
    toStartOfMinute(datetime_mes) AS fenetre,
    avgState(vibration * vibration) AS rms_vibration_state,
    avgState(pressure) AS pression_state
FROM automeca.telemetrie
GROUP BY machine_id, fenetre;

-- ---------------------------------------------------------------------
-- 3) Vue de lecture — fusionne les etats partiels, calcule le RMS reel
-- ---------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS automeca.v_telemetrie_fenetre_1min AS
SELECT
    machine_id,
    fenetre,
    sqrt(avgMerge(rms_vibration_state)) AS rms_vibration,
    avgMerge(pression_state) AS pression_moyenne
FROM automeca.telemetrie_fenetre_1min
GROUP BY machine_id, fenetre;

-- ---------------------------------------------------------------------
-- 4) Vue de lecture — tendance de pression : ecart avec la fenetre
--    precedente de la meme machine (fonction fenetre lagInFrame).
--    Sur la toute premiere fenetre d'une machine (pas de precedente),
--    le defaut vaut la valeur courante elle-meme -> tendance = 0, pour
--    eviter une fausse tendance egale a la pression brute (lagInFrame
--    renvoie 0 par defaut sans ce 3e argument, ce qui declencherait une
--    fausse alerte des la premiere mesure de chaque machine).
-- ---------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS automeca.v_telemetrie_tendance AS
SELECT
    machine_id,
    fenetre,
    rms_vibration,
    pression_moyenne,
    pression_moyenne - lagInFrame(pression_moyenne, 1, pression_moyenne) OVER (
        PARTITION BY machine_id ORDER BY fenetre
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS tendance_pression
FROM automeca.v_telemetrie_fenetre_1min
ORDER BY machine_id, fenetre;
