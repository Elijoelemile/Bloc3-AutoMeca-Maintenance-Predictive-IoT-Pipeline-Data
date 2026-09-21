-- =====================================================================
-- Bloc 3 — Horodatage d'ingestion (ClickHouse)
--
-- Ne modifie pas le DDL d'origine du Bloc 2 (03_telemetrie_clickhouse.
-- sql) : ajoute seulement une colonne, via une migration propre au
-- Bloc 3 (meme logique que les prerequis precedents).
--
-- datetime_mes est l'horodatage de la MESURE (quand le capteur a pris
-- la valeur) — rien n'enregistrait jusqu'ici quand la ligne devient
-- disponible (inseree dans ClickHouse), pourtant necessaire pour la
-- metrique "Latence bout-en-bout" du tableau de bord (page 2). DEFAULT
-- now() se remplit automatiquement a l'insertion, sans modifier
-- ext_load_streaming/kafka_to_clickhouse.py (la colonne n'est pas
-- listee dans l'INSERT, ClickHouse applique le defaut lui-meme).
--
-- A executer UNE SEULE FOIS, apres le DDL du Bloc 2.
-- =====================================================================

ALTER TABLE automeca.telemetrie
    ADD COLUMN horodatage_ingestion DateTime DEFAULT now();
