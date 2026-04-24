# ice-keeper

Les tables [Apache Iceberg](https://iceberg.apache.org/) nécessitent une maintenance régulière. Cela peut être inattendu pour de nombreuses personnes qui découvrent l'architecture de données basée sur Iceberg.

Il y a trois bonnes raisons :

Iceberg permet des mises à jour en arrière-plan – Iceberg résout le problème de la coordination sécurisée de plusieurs écrivains. Cela permet de décomposer les problèmes en éléments plus simples et plus fiables. Auparavant, les écrivains devaient équilibrer la mise à disposition rapide des données (écritures fréquentes) avec les problèmes de performance liés aux petits fichiers, et idéalement aussi regrouper les données pour la consommation en aval. Avec Iceberg, un écrivain en streaming peut rendre les données disponibles rapidement et une tâche de maintenance en arrière-plan peut regrouper et compacter pour des performances à long terme.

Iceberg utilise une approche optimiste. Les écrivains créent des instantanés parallèles d'une table et utilisent un échange atomique pour passer de l'un à l'autre. Les anciens instantanés doivent être conservés jusqu'à ce que les lecteurs ne les utilisent plus. L'inconvénient de ce modèle est que les instantanés doivent être nettoyés plus tard, sinon les anciens fichiers de données pourraient s'accumuler indéfiniment.

En résumé, la maintenance des tables est inévitable dans les formats modernes et, dans de nombreux cas, décomposer le travail en écritures distinctes et en maintenance des données est un meilleur modèle opérationnel.

Voici les opérations les plus courantes nécessaires pour maintenir les tables performantes et rentables avec un minimum d'effort :

- La compaction des données réécrit de manière asynchrone les fichiers de données pour résoudre le problème des petits fichiers, mais peut également regrouper les données pour améliorer les performances des requêtes et supprimer les lignes qui ont été supprimées de manière logique.
- L'expiration des instantanés supprime les anciens instantanés et les fichiers de données qui ne sont plus nécessaires.
- Le nettoyage des fichiers orphelins identifie et supprime les fichiers de données qui ont été écrits mais jamais validés en raison d'échecs de tâches.

ice-keeper est un service qui automatise la maintenance des tables Iceberg. ice-keeper est programmé pour s'exécuter chaque nuit dans Airflow/Spellbook.

ice-keeper peut :
- expirer les anciens instantanés
- trouver et supprimer les fichiers orphelins (non suivis par Iceberg)
- exécuter une optimisation sur les partitions non saines pour améliorer les performances de recherche

## Configuration d'ice-keeper via les propriétés des tables

Les propriétaires de tables peuvent contrôler ce que ice-keeper fera avec leur table. Ils peuvent s'inscrire, se désinscrire et configurer tous les aspects de la maintenance automatisée des tables. Les paramètres d'ice-keeper sont gérés à l'aide des propriétés des tables Iceberg.

Les propriétaires de tables peuvent définir les propriétés des tables en utilisant l'appel SQL suivant :
```sql
alter table my_catalog.my_schema.my_tableset set tblproperties (
    'ice-keeper.notification-email'='mon-email@domain.gc.ca'
)
```

Consultez la [documentation](https://iceberg.apache.org/docs/latest/spark-ddl/#alter-table-set-tblproperties) d'Iceberg pour plus de détails. Le tableau ci-dessous répertorie toutes les configurations disponibles pour ice-keeper.

| Propriété de la table                           | Valeur par défaut  | Description                |
| ----------------------------------------------- | ------------------ | -------------------------- |
| ice-keeper.notification-email                  | Aucun              | Spécifie une adresse e-mail pour recevoir des notifications en cas d'échecs. Cette propriété garantit que des alertes sont envoyées à l'e-mail configuré lorsque des actions de maintenance rencontrent des problèmes ou des erreurs. |
| ice-keeper.should-expire-snapshots             | true               | Détermine si une table doit participer au processus d'expiration des instantanés d'ice-keeper. |
| ice-keeper.retention-days-snapshots            | 7                  | Définit le nombre de jours pendant lesquels les instantanés doivent être conservés. |
| history.expire.max-snapshot-age-ms             | 604800000 (7 jours)| Configuration native d'Iceberg où la résolution est en millisecondes. Cependant, ice-keeper arrondit à l'entier inférieur en jours. Il est recommandé d'utiliser `ice-keeper.retention-days-snapshots` pour une gestion plus simple. |
| ice-keeper.retention-num-snapshots             | 60                 | Définit le nombre minimum d'instantanés à conserver. |
| history.expire.min-snapshots-to-keep           | 60                 | Configuration native d'Iceberg, mais il est conseillé d'utiliser `ice-keeper.retention-num-snapshots` à la place. |
| ice-keeper.should-remove-orphan-files          | true               | Détermine si une table doit subir le processus de suppression des fichiers orphelins d'ice-keeper. |
| ice-keeper.retention-days-orphan-files         | 5                  | Indique que les fichiers orphelins de moins d'un certain nombre de jours ne doivent pas être supprimés. |
| ice-keeper.should-optimize                     | false              | Indique si une table doit être optimisée à l'aide de stratégies binpack, sort ou Z-order. |
| ice-keeper.min-age-to-optimize                 | -1 (obsolète)      | **Obsolète.** Utilisez `ice-keeper.min-partition-to-optimize` à la place. Nombre de partitions récentes à ignorer lors de l'optimisation. |
| ice-keeper.max-age-to-optimize                 | -1 (obsolète)      | **Obsolète.** Utilisez `ice-keeper.max-partition-to-optimize` à la place. Nombre de partitions à considérer pour l'optimisation. |
| ice-keeper.min-partition-to-optimize            | 1d                 | Décalage temporel minimum de la partition (par rapport à la partition la plus récente) pour commencer l'optimisation. Spécifié sous forme de chaîne de durée avec une valeur numérique et un suffixe d'unité : `d` (jour), `h` (heure), `m` (mois), `y` (année). Par exemple, `1d` signifie ignorer le jour le plus récent, `0m` signifie inclure le mois en cours. Supporte les valeurs négatives (ex : `-1d`). |
| ice-keeper.max-partition-to-optimize            | 7d                 | Décalage temporel maximum de la partition (par rapport à la partition la plus récente) à considérer pour l'optimisation. Utilise le même format de durée que `ice-keeper.min-partition-to-optimize`. **Lors d'une mise à niveau depuis une version antérieure qui n'utilisait pas ces paramètres, assurez-vous d'exécuter la commande de réinitialisation de la maintenance schedule ou d'appliquer un `ALTER TABLE` sur la table `maintenance_schedule` pour ajouter les nouvelles colonnes correspondantes avant de relancer ice-keeper, afin d'éviter les échecs de la logique `MERGE`.** |
| ice-keeper.optimization-strategy               | Aucun              | Définit la stratégie d'optimisation à utiliser. |
| ice-keeper.optimize-partition-depth            | 1                  | Par défaut, ice-keeper analyse et optimise les partitions au premier niveau. |
| ice-keeper.sort-corr-threshold                 | -1 (désactivé)     | **Pour le débogage/test uniquement — ne devrait pas être défini par les utilisateurs en fonctionnement normal.** Lorsque défini à une valeur >= 0, remplace le seuil de corrélation par défaut utilisé par toutes les stratégies d'optimisation (sort : 0.97, binpack : 1.00, zorder : courbe dynamique basée sur le nombre de fichiers). Le drapeau `should_sort` d'une partition est activé lorsque `corr < corr_threshold`. Défini à `2.0` dans les tests d'intégration pour forcer `should_sort = true` avec un minimum de données puisque `corr` plafonne à 1.0. |
| ice-keeper.optimization-target-file-size-bytes  | -1 (auto)          | Spécifie la taille cible des fichiers lors de l'exécution du processus d'optimisation via `rewrite_data_files`. La valeur par défaut de `-1` active le dimensionnement automatique de la taille cible par partition en fonction de la taille totale des données (de 16 Mo à 1 Go). Définir à une valeur en octets spécifique (ex : `536870912` pour 512 Mo) pour utiliser une taille cible fixe pour toutes les partitions. Le dimensionnement automatique **nécessite** que `ice-keeper.optimize-partition-depth` soit égal au nombre de niveaux de partition de la table ou soit défini à `-1` (regroupement dynamique). |
| write.target-file-size-bytes                   | 536870912 (512 Mo)  | Propriété native d'Iceberg. Utilisée comme valeur de repli lorsque `ice-keeper.optimization-target-file-size-bytes` n'est **pas** défini. Si les deux sont présents, la propriété ice-keeper a la priorité. |
| ice-keeper.should-rewrite-manifest             | false              | Détermine si ice-keeper doit exécuter la procédure `rewrite_manifest`. |
| ice-keeper.should-apply-lifecycle              | false              | Spécifie si ice-keeper doit supprimer automatiquement les lignes avec des données plus anciennes. |
| ice-keeper.lifecycle-max-days                  | 330                | Définit le nombre maximum de jours pour conserver les données. |
| ice-keeper.lifecycle-ingestion-time-column     | Aucun              | Spécifie la colonne à utiliser comme horodatage d'ingestion pour les opérations de cycle de vie. |
| ice-keeper.widening.rule.src.partition         | Aucun              | Nom de la partition source à élargir. |
| ice-keeper.widening.rule.dst.partition         | Aucun              | Nom de la partition de destination élargie. |
| ice-keeper.widening.rule.min.age.to.widen      | -1                  | **Obsolète.** Utilisez `ice-keeper.widening.rule.min-partition-to-widen` à la place. Âge minimum pour qu'un fichier de données soit éligible à l'élargissement. |
| ice-keeper.widening.rule.min-partition-to-widen | 1M                 | Décalage temporel minimum de la partition (par rapport à la partition la plus récente) pour commencer l'élargissement. Utilise le même format de durée que `ice-keeper.min-partition-to-optimize` (ex : `1d`, `0m`, `2m`). |
| ice-keeper.widening.rule.max-partition-to-widen | 2M                 | Décalage temporel maximum de la partition (par rapport à la partition la plus récente) à considérer pour l'élargissement. Utilise le même format de durée que `ice-keeper.min-partition-to-optimize` (ex : `7d`, `12m`). |
| ice-keeper.widening.rule.select.criteria       | Aucun              | Critères de sélection des lignes pour l'élargissement. |
| ice-keeper.widening.rule.required_partition_columns | Aucun          | Liste des colonnes qui ne doivent pas contenir de valeurs NULL avant l'élargissement. |

### Dimensionnement automatique de la taille cible des fichiers

Lorsque `ice-keeper.optimization-target-file-size-bytes` est défini à `-1` (la valeur par défaut), ice-keeper sélectionne automatiquement une taille cible de fichier par partition en fonction de la taille totale des données de cette partition. La taille cible est adaptée de sorte que chaque partition contienne au maximum N fichiers de N Mo chacun.

| Taille de la partition  | Taille cible des fichiers | Nombre max de fichiers par partition |
| ----------------------- | ------------------------- | ------------------------------------ |
| < 256 Mo                | 16 Mo                     | 16                                   |
| < 1 Go                  | 32 Mo                     | 32                                   |
| < 4 Go                  | 64 Mo                     | 64                                   |
| < 16 Go                 | 128 Mo                    | 128                                  |
| < 64 Go                 | 256 Mo                    | 256                                  |
| < 256 Go                | 512 Mo                    | 512                                  |
| ≥ 256 Go                | 1 Go                      | —                                    |

Ceci nécessite que `ice-keeper.optimize-partition-depth` soit égal au nombre de niveaux de partition de la table, ou soit défini à `-1` (regroupement dynamique), afin que chaque sous-partition soit dimensionnée indépendamment.