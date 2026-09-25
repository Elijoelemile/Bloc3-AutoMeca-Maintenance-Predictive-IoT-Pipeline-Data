-- =====================================================================
-- BLOC 2 — AutoMeca Systems — Télémétrie (série temporelle)
-- ClickHouse auto-hébergé (petite instance Compute, région UE — voir
-- README pour la justification face au tarif du ClickHouse managé)
--
-- Source : PdM_telemetry.csv (876 100 lignes, 100 machines, mesures
-- horaires sur 1 an). Volume et cadence trop élevés pour un modèle en
-- étoile classique côté PostgreSQL : table dédiée, moteur colonnaire,
-- triée physiquement pour les requêtes par machine/période.
-- =====================================================================

CREATE DATABASE IF NOT EXISTS automeca;

CREATE TABLE automeca.telemetrie
(
    machine_id   UInt16,
    datetime_mes DateTime,
    volt         Float64,
    rotate       Float64,
    pressure     Float64,
    vibration    Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(datetime_mes)
ORDER BY (machine_id, datetime_mes);

-- Partitionnement mensuel + tri (machine_id, datetime_mes) : les deux
-- requêtes dominantes ("historique d'une machine" et "état du parc sur
-- une période donnée") lisent uniquement les partitions/plages utiles
-- au lieu de scanner l'ensemble des 876 100 lignes.
