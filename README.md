# ice-keeper

ice-keeper is a service that automates Iceberg table maintenance. ice-keeper is scheduled to run every night in Airflow/Spellbook.

ice-keeper can:
- expire old snapshots
- find and remove orphan files (not tracked by Iceberg)
- run an optimization on unhealthy partitions to improve search performance

[user documentation](./docs/USERS.md)

[developer documentation](./docs/DEVELOPERS.md)

---

# ice-keeper (Français)

ice-keeper est un service qui automatise la maintenance des tables Iceberg. ice-keeper est programmé pour s'exécuter chaque nuit dans Airflow/Spellbook.

ice-keeper peut :
- expirer les anciens instantanés
- trouver et supprimer les fichiers orphelins (non suivis par Iceberg)
- exécuter une optimisation sur les partitions non saines pour améliorer les performances de recherche

[documentation utilisateur](./docs/USERS-FR.md)

[documentation développeur](./docs/DEVELOPERS-FR.md)
