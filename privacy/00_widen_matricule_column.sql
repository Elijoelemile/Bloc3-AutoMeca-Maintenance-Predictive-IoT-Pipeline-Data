-- =====================================================================
-- Bloc 3 — Elargissement de datamart.dim_operateur.matricule
--
-- Ne modifie pas le DDL d'origine du Bloc 2 (02_datamart.sql) : ajuste
-- seulement le type d'une colonne, via une migration propre au Bloc 3
-- (meme logique que transform/00_add_missing_constraints.sql).
--
-- matricule etait defini en VARCHAR(20) pour un identifiant lisible
-- (ex. "OP-042"). privacy/pseudonymisation.py y ecrit desormais un
-- condensat HMAC-SHA256 (64 caracteres hexadecimaux) — VARCHAR(20) le
-- tronquerait. nom (VARCHAR(100)) n'a pas ce probleme (64 < 100).
--
-- A executer UNE SEULE FOIS, apres le DDL du Bloc 2, avant toute
-- ecriture pseudonymisee dans dim_operateur.
-- =====================================================================

ALTER TABLE datamart.dim_operateur
    ALTER COLUMN matricule TYPE VARCHAR(64);
