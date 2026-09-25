-- =====================================================================
-- Bloc 3 — Table de quarantaine (ClickHouse)
--
-- Recoit les mesures rejetees par la regle "valeur hors plage" (voir
-- quality/validation_rules.py), appliquee dans ext_load_streaming/
-- kafka_to_clickhouse.py avant le chargement dans automeca.telemetrie.
-- Jamais fusionnee avec la table brute — permet aussi de calculer le
-- "taux d'erreur" du tableau de bord (page 2) par une simple requete
-- SQL : count(quarantaine) / (count(quarantaine) + count(telemetrie)).
--
-- A executer UNE SEULE FOIS, apres le DDL du Bloc 2
-- (03_telemetrie_clickhouse.sql), avant la premiere execution du
-- consommateur Kafka -> ClickHouse.
-- =====================================================================

CREATE TABLE automeca.telemetrie_quarantaine
(
    machine_id              UInt16,
    datetime_mes            DateTime,
    volt                    Float64,
    rotate                  Float64,
    pressure                Float64,
    vibration                Float64,
    motif                    LowCardinality(String),   -- ex. 'volt_hors_plage'
    horodatage_quarantaine  DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(datetime_mes)
ORDER BY (machine_id, datetime_mes);
