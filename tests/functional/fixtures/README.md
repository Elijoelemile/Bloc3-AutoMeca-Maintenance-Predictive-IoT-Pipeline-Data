# Fixtures — copie figée du DDL du Bloc 2

Ces fichiers sont une **copie autonome**, pas une dépendance technique
au dépôt [Bloc2-AutoMeca-Maintenance-Predictive-IoT-Architecture-Data](https://github.com/<user>/Bloc2-AutoMeca-Maintenance-Predictive-IoT-Architecture-Data)
— nécessaire pour que `tests/functional/` tourne à partir d'un simple
clone de ce dépôt, sans dépendre d'un chemin relatif vers un autre
dépôt sur le disque.

**Limite assumée** : si le DDL réel du Bloc 2 change, cette copie ne
se met pas à jour automatiquement. À resynchroniser manuellement si
besoin (copie identique, aucune modification).

Copiés le 2026-09-21 depuis `database_scripts/01_staging.sql`,
`02_datamart.sql` et `03_telemetrie_clickhouse.sql`.
