# ice-keeper

La bibliothèque Iceberg fournit des procédures stockées dans Spark pour la maintenance des tables. La plupart du temps, ces opérations relèvent de la responsabilité des administrateurs de la plateforme de données.

ice-keeper est un outil CLI pour automatiser la maintenance des tables Iceberg.

ice-keeper peut :

- découvrir de nouvelles tables à gérer
- expirer les anciens instantanés
- trouver et supprimer les fichiers orphelins (non suivis par Iceberg)
- exécuter une optimisation sur les partitions non saines pour améliorer les performances de recherche

ice-keeper est conçu pour effectuer la maintenance de centaines de tables simultanément et mieux utiliser les ressources Spark.

ice-keeper est programmé pour s'exécuter chaque nuit dans Airflow/Spellbook.

ice-keeper s'inspire de cet article [Automated Table Maintenance for Apache Iceberg Tables](https://www.starburst.io/blog/automated-table-maintenance-for-apache-iceberg/) et du [script GitHub associé](https://github.com/mdesmet/trino-iceberg-maintenance/blob/main/trino_iceberg_maintenance/__main__.py).

## Démarrage

ice-keeper s'exécute depuis la ligne de commande et nécessite un argument d'action. La syntaxe est la suivante : `ice-keeper <action>`. Les actions disponibles incluent :

| Nom de l'action         | Description                                                                                                          |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------- |
| **schedule**            | Afficher ou modifier le calendrier de maintenance.                                                                   |
| **discover**            | Identifier de nouvelles tables Apache Iceberg à gérer et mettre à jour les configurations des tables déjà suivies par ice-keeper. |
| **optimize**            | Améliorer les performances des tables à l'aide de stratégies binpack ou sort.                                        |
| **expire**              | Supprimer les instantanés obsolètes pour préserver les performances et gérer le stockage.                            |
| **orphans**             | Nettoyer les fichiers de données ou de métadonnées orphelins qui ne sont plus référencés.                            |
| **rewrite_manifest**    | Réorganiser et optimiser les fichiers manifest pour une meilleure efficacité.                                        |
| **journal**             | Afficher les journaux des opérations telles que `optimize`, `expire`, `orphans` et `rewrite_manifest`.               |

ice-keeper prend en charge une variété d'arguments optionnels, permettant de personnaliser les actions. L'utilisation est la suivante : `ice-keeper <action> [options]`

| Argument optionnel           | Description                                                                                                        |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| **--catalog**                | Restreindre la portée de l'action au catalogue spécifié.                                                           |
| **--schema**                 | Restreindre la portée de l'action au schéma spécifié.                                                              |
| **--table_name**             | Restreindre la portée de l'action à la table spécifiée.                                                            |
| **--where**                  | Appliquer un filtre ad hoc pour déterminer la portée de l'action, par exemple `--where "full_name = 'dev_catalog.schema2.table4'"`. |
| **--set**                    | Utilisé exclusivement par l'action `schedule` pour spécifier les colonnes à modifier et leurs nouvelles valeurs.    |
| **--spark_executors**        | Définir le nombre d'exécuteurs Spark souhaités. Mettre cette valeur à zéro exécutera le processus uniquement avec un driver Spark. |
| **--spark_executor_cores**   | Spécifier le nombre de cœurs CPU alloués à chaque instance d'exécuteur.                                            |
| **--spark_executor_memory**  | Définir la taille de la RAM allouée à chaque exécuteur, par exemple `16g`.                                         |
| **--spark_driver_cores**     | Spécifier le nombre de cœurs CPU alloués au driver Spark.                                                          |
| **--spark_driver_memory**    | Définir la taille de la RAM allouée au driver Spark, par exemple `8g`.                                             |
| **--concurrency**            | Définir le nombre de tables à traiter en parallèle.                                                                |

## Action : discover

L'exécution du processus de découverte remplira la table `maintenance_schedule`.

```bash
./ice-keeper discover --catalog dev_catalog --schema jcc
```

Le processus de découverte demande à ice-keeper de scanner tous les catalogues et schémas. Pour chaque nouvelle table détectée, il crée une nouvelle entrée dans le calendrier de maintenance d'ice-keeper. Si l'entrée existe déjà, ice-keeper s'assure que toutes les modifications utilisateur sont prises en compte. Les modifications utilisateur sont des propriétés spécifiques à ice-keeper.

Inversement, pour toute table qui avait une entrée dans le calendrier de maintenance mais qui n'est plus présente dans le catalogue Iceberg, ice-keeper supprimera cette entrée de son calendrier de maintenance.

En résumé, l'action discover synchronise les informations trouvées dans le catalogue avec son calendrier de maintenance.

### Comment fonctionne l'action discover

Pour expliquer le processus de découverte, nous utiliserons une seule configuration, à savoir `should_expire_snapshots`. Cette configuration est définie sur true par défaut, sauf si l'utilisateur la remplace spécifiquement par une propriété `ice-keeper.should-expire-snapshots`.

| Configuration           | Propriété tblproperty              | Valeur par défaut |
| ----------------------- | ---------------------------------- | ----------------- |
| should_expire_snapshots | ice-keeper.should-expire-snapshots | true              |

Supposons qu'un utilisateur ait écrit une analyse et stocke les résultats dans une table Iceberg appelée `cyber_detections`. La table pourrait avoir été créée comme suit :

```sql
create or replace table dev_catalog.jcc.cyber_detections
(event_time timestamp, id long, col1 string)
using iceberg
partitioned by (days(event_time))
tblproperties(
   'write.format.default'='parquet'
)
```

L'exécution de cette commande `ice-keeper discover --catalog dev_catalog` lancera le processus de découverte. ice-keeper trouvera cette nouvelle table et l'inclura dans son calendrier de maintenance. Nous voyons que `should_expire_snapshots` est défini sur true car c'est la valeur par défaut pour cette configuration.

| full_name          | should_expire_snapshots | retention_days_snapshots | should_remove_orphan_files | retention_days_orphan_files |
| ------------------ | ----------------------- | ------------------------ | -------------------------- | --------------------------- |
| ..cyber_detections | true                    | 30                       | true                       | 5                           |

Chaque nuit, ice-keeper est lancé pour exécuter le processus d'expiration des instantanés en utilisant le calendrier de maintenance.

Si un utilisateur souhaite se désinscrire de ce comportement, il peut remplacer la configuration en créant sa table comme suit :

```sql
create or replace table dev_catalog.jcc.cyber_detections
(event_time timestamp, id long, col1 string)
using iceberg
partitioned by (days(event_time))
tblproperties(
   'write.format.default'='parquet',
   'ice-keeper.should-expire-snapshots'='false'
)
```

Ou, à tout moment, il peut modifier les propriétés de la table comme suit :

```sql
alter table dev_catalog.jcc.cyber_detections
set tblproperties ('ice-keeper.should-expire-snapshots'='false')
```

Il est facile de vérifier les propriétés tblproperties d'une table Iceberg :

```sql
show tblproperties dev_catalog.jcc.cyber_detections
```

Pour supprimer une propriété tblproperty :

```sql
alter table dev_catalog.jcc.cyber_detections
unset tblproperties ('ice-keeper.should-expire-snapshots')
```

## Affichage des modifications du calendrier de maintenance

Iceberg enregistre les modifications apportées aux tables via un historique des instantanés (commits). Nous pouvons exploiter cette fonctionnalité pour inspecter les modifications apportées au calendrier de maintenance. Ces modifications peuvent être effectuées via le processus de découverte ou manuellement par un administrateur. Dans tous les cas, une modification est apportée au calendrier de maintenance et peut donc être récupérée via la procédure Iceberg [create_changelog_view](https://iceberg.apache.org/docs/nightly/spark-procedures/#create_changelog_view).

Supposons que nous ayons exécuté cette commande pour mettre à jour le calendrier de maintenance :

```bash
./ice-keeper \
   --where " full_name = 'dev_catalog.admin.ice_keeper_maintenance_schedule' " \
   schedule \
   --set " retention_days_snapshots = 90 " \
```

Pour trouver les modifications effectuées dans la dernière heure, nous créons une vue des modifications en spécifiant un horodatage de début.

```sql
%%sparksql
CALL dev_catalog.system.create_changelog_view(
  table => 'admin.ice_keeper_maintenance_schedule',
  options => map('start-timestamp','1736881275000'),
  changelog_view => 'ice_keeper_maintenance_schedule_changes',
  identifier_columns => array('catalog', 'schema', 'table_name', 'full_name')
)
```

Nous pouvons ensuite afficher et interroger cette vue :

```sql
%%sparksql
select
  full_name,
  retention_days_snapshots,
  _change_type,
  _change_ordinal,
  _commit_snapshot_id
from
  ice_keeper_maintenance_schedule_changes
order by
  _change_ordinal asc,
  _change_type desc
```

Cela affichera les modifications apportées à la colonne `retention_days_snapshots` :

| last_updated_by                          | retention_days_snapshots | _change_type   | _change_ordinal | _commit_snapshot_id |
| --------------------------------------- | ------------------------ | -------------- | ---------------- | -------------------- |
| jupyhub/jcc    | 91                       | UPDATE_BEFORE  | 0                | 4563331490714018710  |
| jupyhub/jcc    | 90                       | UPDATE_AFTER   | 0                | 4563331490714018710  |

La procédure `create_changelog_view` ajoute 3 colonnes supplémentaires (`_change_type`, `_change_ordinal`, `_commit_snapshot_id`) qui sont expliquées en détail [ici](https://iceberg.apache.org/docs/nightly/spark-procedures/#create_changelog_view).

Si nous voulons voir l'heure de `committed_at` plutôt que l'ID de l'instantané, nous pouvons joindre la table de métadonnées `.snapshot`.

```sql
%%sparksql
select
  full_name,
  retention_days_snapshots,
  _change_type,
  _change_ordinal,
  s.committed_at
from
  ice_keeper_maintenance_schedule_changes as c
  left join dev_catalog.admin.ice_keeper_maintenance_schedule.snapshots as s
  on (c._commit_snapshot_id = s.snapshot_id)
order by
  _change_ordinal asc,
  _change_type desc
```

## L'Action Journal

En plus d'utiliser le mécanisme de journalisation Python, ice-keeper écrit également le résultat de chaque action individuelle effectuée sur les tables gérées. Toutes les actions sont enregistrées dans la table `journal`. Les actions utilisent un ensemble commun de colonnes, et certaines colonnes sont spécifiques à l'action.

| Nom de colonne commune | Description                                                                                                |
| ---------------------- | ---------------------------------------------------------------------------------------------------------- |
| full_name              | Nom de la table sur laquelle l'action a été effectuée.                                                     |
| start_time             | Heure de début de l'action.                                                                                |
| end_time               | Heure de fin de l'action.                                                                                  |
| exec_time_seconds      | Temps d'exécution.                                                                                         |
| sql_stm                | L'instruction SQL exécutée, par exemple : call catalog.rewrite_data_files.                                 |
| status                 | Statut de l'exécution. SUCCESS ou FAILED suivi de la trace de l'exception.                                 |
| executed_by            | Identité ayant exécuté cette action.                                                                       |
| action                 | Action effectuée, cela peut être rewrite_data_files, expire_snapshots, rewrite_manifests, remove_orphan_files |

| Utilisé par rewrite_data_files | Description        |
| ----------------------------- | ------------------ |
| rewritten_data_files_count    | rewrite_data_files |
| added_data_files_count        | rewrite_data_files |
| rewritten_bytes_count         | rewrite_data_files |
| failed_data_files_count       | rewrite_data_files |

| Utilisé par expire_snapshots            | Description      |
| --------------------------------------- | ---------------- |
| deleted_data_files_count                | expire_snapshots |
| deleted_position_delete_files_count     | expire_snapshots |
| deleted_equality_delete_files_count     | expire_snapshots |
| deleted_manifest_files_count            | expire_snapshots |
| deleted_manifest_lists_count            | expire_snapshots |
| deleted_statistics_files_count          | expire_snapshots |

| Utilisé par rewrite_manifests | Description       |
| ----------------------------- | ----------------- |
| rewritten_manifests_count     | rewrite_manifests |
| added_manifests_count         | rewrite_manifests |

| Utilisé par remove_orphan_files | Description                                    |
| ------------------------------- | ---------------------------------------------- |
| num_orphan_files_deleted        | Nombre de fichiers supprimés par remove_orphan_files |

_Tableau 2 : table journal_

Le journal peut être affiché à l'aide de l'action `journal`. Cette commande affichera les exécutions de expire_snapshots sur schema1.

```bash
./ice-keeper journal \
    --where " catalog = 'dev_catalog' and schema = 'schema1' and action = 'expire_snapshots' "
```

## L'Action Schedule

Le calendrier de maintenance peut être affiché à l'aide de l'action `schedule`. Cette commande affichera le calendrier de maintenance des tables dev_catalog.schema1.

```bash
./ice-keeper schedule \
    --where " catalog = 'dev_catalog' and schema = 'schema1' and table_name like 'telemetry%' "
```

## L'Action Expire

Dans Apache Iceberg, chaque modification des données d'une table crée une nouvelle version, appelée un instantané. Les métadonnées Iceberg suivent plusieurs instantanés en même temps pour permettre aux lecteurs utilisant d'anciens instantanés de terminer, pour permettre une consommation incrémentielle et pour les requêtes de voyage dans le temps.

Bien sûr, conserver toutes les données de table indéfiniment n'est pas pratique. Une partie de la maintenance de base des tables Iceberg consiste à expirer les anciens instantanés pour garder les métadonnées des tables petites et éviter les coûts de stockage élevés des fichiers de données inutiles. Les instantanés s'accumulent jusqu'à ce qu'ils soient expirés.

L'expiration est configurée avec deux paramètres :

- Âge maximum des instantanés : Une fenêtre temporelle au-delà de laquelle les instantanés sont supprimés.
- Nombre minimum d'instantanés à conserver : Un nombre minimum d'instantanés à conserver dans l'historique. À mesure que de nouveaux instantanés sont ajoutés, les plus anciens sont supprimés.

Cette commande exécute l'action expire, qui exécute essentiellement la procédure Iceberg expire_snapshots.

```bash
./ice-keeper expire --where " full_name = 'dev_catalog.schema1.telemetry_1' "
```

## L'Action Orphan

Le nettoyage des fichiers orphelins — fichiers de données qui ne sont pas référencés par les métadonnées de la table — est une partie importante de la maintenance des tables qui réduit les coûts de stockage.

### Que sont les fichiers orphelins et comment sont-ils créés ?

Les fichiers orphelins sont des fichiers dans le répertoire de données de la table qui ne font pas partie de l'état de la table. Comme leur nom l'indique, les fichiers orphelins ne sont pas suivis par Iceberg, ne sont référencés par aucun instantané dans le journal des instantanés d'une table et ne sont pas utilisés par les requêtes.

Les fichiers orphelins proviennent des échecs dans les systèmes distribués qui écrivent dans les tables Iceberg. Par exemple, si un driver Spark manque de mémoire et se bloque après que certaines tâches ont réussi à créer des fichiers de données, ces fichiers resteront dans le stockage, mais ne seront jamais validés dans la table.

#### Le défi des fichiers orphelins

Les fichiers orphelins s'accumulent avec le temps ; s'ils ne sont pas référencés dans les métadonnées de la table, ils ne peuvent pas être supprimés par l'expiration normale des instantanés. À mesure qu'ils s'accumulent, les coûts de stockage continuent d'augmenter, il est donc conseillé de les trouver et de les supprimer régulièrement. La meilleure pratique recommandée est d'exécuter un nettoyage des fichiers orphelins chaque semaine ou chaque mois.

Supprimer les fichiers orphelins peut être délicat. Cela nécessite de comparer l'ensemble complet des fichiers référencés dans une table à l'ensemble actuel des fichiers dans le magasin d'objets sous-jacent. Cela en fait également une opération gourmande en ressources, en particulier si vous avez un grand volume de fichiers dans les répertoires de données et de métadonnées.

De plus, les fichiers peuvent sembler orphelins lorsqu'ils font partie d'une opération de validation en cours. Iceberg utilise une concurrence optimiste, donc les écrivains créeront tous les fichiers qui font partie d'une opération avant la validation. Jusqu'à ce que la validation réussisse, les fichiers ne sont pas référencés. Pour éviter de supprimer des fichiers qui font partie d'une validation en cours, les procédures de maintenance utilisent un argument olderThan. Par défaut, cette fenêtre temporelle est de 3 jours, ce qui devrait être largement suffisant pour que les validations en cours réussissent.

Cette commande exécute l'action expire, qui exécute la procédure Iceberg `remove_orphan_files`.

```bash
./ice-keeper expire --where " full_name = 'dev_catalog.schema1.telemetry_1' "
```

## L'Action Rewrite Manifest

Cette commande exécute l'action rewrite_manifest, qui exécute la procédure Iceberg `rewrite_manifest`.

```bash
./ice-keeper rewrite_manifest --where " full_name = 'dev_catalog.schema1.telemetry_1' "
```

## L'Action Optimize

La motivation principale pour créer Apache Iceberg était de rendre les transactions sûres et fiables. Sans écritures concurrentes sûres, les pipelines n'ont qu'une seule opportunité d'écrire des données dans une table. Les changements inutiles sont risqués : les requêtes pourraient produire des résultats à partir de mauvaises données et les écrivains pourraient corrompre définitivement une table. En bref, les tâches d'écriture sont responsables de trop de choses et doivent faire des compromis — conduisant souvent à des problèmes de performance persistants comme le problème des "petits fichiers".

Avec les mises à jour fiables qu'offre Iceberg, vous pouvez décomposer la préparation des données en tâches distinctes. Les écrivains sont responsables de la transformation et de la mise à disposition rapide des données. Les optimisations de performance comme la compaction sont appliquées plus tard en tant que tâches en arrière-plan. Et il s'avère que ces tâches différées sont réutilisables et également plus faciles à appliquer lorsqu'elles sont séparées de la logique de transformation personnalisée.

La compaction des fichiers n'est pas seulement une solution au problème des petits fichiers. La compaction réécrit les fichiers de données, ce qui est une opportunité pour également reclasser, repartitionner et supprimer les lignes supprimées.

L'action optimize recherchera des fichiers dans toute partition qui sont soit trop petits, soit trop grands, et les réécrira à l'aide d'un algorithme de bin packing pour combiner les fichiers. Elle tente de produire des fichiers de la taille cible de la table, contrôlée par la propriété de table write.target-file-size-bytes, qui est par défaut de 512 Mo.

Voici comment invoquer l'action optimize :

```bash
./ice-keeper optimize  --where " full_name = 'dev_catalog.schema1.telemetry_1' "
```

En interne, l'action optimize évalue les informations sur la santé des partitions pour déterminer quelles partitions doivent être optimisées.

Après avoir exécuté les optimisations, ice-keeper stockera un rapport des informations sur la santé des partitions.

Ces informations sont stockées dans la table `ice_keeper_partition_health`. L'évaluation de la santé dépendra de la valeur de `optimization_strategy`.

Une valeur de `optimization_strategy` définie sur binpack suppose qu'il n'y a pas de tri et ne considérera que le nombre de fichiers correspondant à la taille cible définie par `target_file_size_bytes`.

Toute autre valeur pour `optimization_strategy` sera considérée comme une liste de colonnes à trier. Par exemple, **id** signifierait qu'une partition saine est censée avoir des fichiers de données avec des limites inférieures et supérieures résultant du tri de la table par **id**.

Si `optimization_strategy` contient une chaîne comme **zorder(col1, col2)**, alors l'action diagnose vérifiera que les fichiers de données de la partition sont triés selon **zorder(col1, col2)**.

Le facteur de santé est écrit dans la colonne `corr`. Une valeur de 1.0 signifie que la partition est triée à 100 % selon la stratégie d'optimisation.

| Nom de colonne                        | Description |
| ------------------------------------- | ----------- |
| start_time                            |             |
| full_name string                      |             |
| catalog string                        |             |
| schema string                         |             |
| table_name string                     |             |
| partition_desc                        |             |
| partition_age                         |             |
| n_files_before                        |             |
| n_files_after                         |             |
| num_files_targetted_for_rewrite_before |             |
| num_files_targetted_for_rewrite_after |             |
| n_records_before                      |             |
| n_records_after                       |             |
| avg_file_size_in_mb_before            |             |
| avg_file_size_in_mb_after             |             |
| min_file_size_in_mb_before            |             |
| min_file_size_in_mb_after             |             |
| max_file_size_in_mb_before            |             |
| max_file_size_in_mb_after             |             |
| sum_file_size_in_mb_before            |             |
| sum_file_size_in_mb_after             |             |
| corr_before                           |             |
| corr_after double                     |             |

_Tableau 3 : table partition_health_

## L'Action Diagnosis

Dans le cadre du processus d'optimisation, Ice-Keeper exécute d'abord un diagnostic sur la table pour identifier les partitions nécessitant une optimisation. Vous pouvez invoquer manuellement l'action diagnosis sur une table, même si elle n'est pas encore configurée pour être optimisée. Cela est utile pour vérifier si la table bénéficierait d'une maintenance par Ice-Keeper.

Voici un exemple d'exécution de l'action diagnosis :

```bash
ICEKEEPER_CONFIG=./config/ice-keeper.yaml \
  ./ice-keeper diagnose \
  --full_name dev_catalog.schema1.table1 \
  --max_age_to_diagnose 1000 \
  --optimization_strategy 'address ASC NULLS FIRST, id DESC'
```

La stratégie d'optimisation accepte les valeurs définies dans la propriété tblproperty `ice-keeper.optimization-strategy`.

## Conseils pour les Scripts Bash

Le CLI ice-keeper accepte un argument `--where`. Cela donne à l'administrateur beaucoup de flexibilité pour choisir les tables affectées par la commande émise à ice-keeper. Cependant, rédiger un filtre complexe sur une seule ligne peut être fastidieux. Pour plus de clarté, vous pouvez utiliser des filtres sur plusieurs lignes. Voici comment vous pouvez y parvenir dans un script bash.

Cela définit la variable `where` avec le contenu d'une chaîne multi-lignes, similaire à une chaîne triple guillemets en Python.

```bash
read -r -d '' where << EOM
    catalog in ('dev_catalog')
    and (
      schema like 'xyz%'
      or
      schema like 'abc%'
   )
EOM
```

Vous pouvez ensuite utiliser cette chaîne comme une variable normale.

```bash
./ice-keeper rewrite_manifest \
    --concurrency 32 \
    --spark_driver_cores 8 \
    --spark_driver_memory 8g \
    --spark_executors 5 \
    --spark_executor_cores 10 \
    --spark_executor_memory 10g \
    --where "$where"
```

## Allocation des Ressources Spark

L'action expire exécute à la fois les procédures expire_snapshots et rewrite_manifests. Ces deux procédures n'utilisent pas les workers Spark. Ainsi, nous configurons ice-keeper pour s'exécuter avec `--spark_executors 0`. Cependant, `rewrite_manifests` peut nécessiter beaucoup de mémoire sur certaines tables, nous l'exécutons donc avec une grande quantité de RAM `--spark_driver_memory 32g`.

```bash
./ice-keeper rewrite_manifest \
    --concurrency 32 \
    --spark_driver_cores 8 \
    --spark_driver_memory 8g \
    --spark_executors 5 \
    --spark_executor_cores 10 \
    --spark_executor_memory 10g
```

Après avoir exécuté les optimisations, ice-keeper stockera un rapport des informations sur la santé des partitions.

Ces informations sont stockées dans la table `ice_keeper_partition_health`. L'évaluation de la santé dépendra de la valeur de `optimization_strategy`.

Une valeur de `optimization_strategy` définie sur binpack suppose qu'il n'y a pas de tri et ne considérera que le nombre de fichiers correspondant à la taille cible définie par `target_file_size_bytes`.

Toute autre valeur pour `optimization_strategy` sera considérée comme une liste de colonnes à trier. Par exemple, **id** signifierait qu'une partition saine est censée avoir des fichiers de données avec des limites inférieures et supérieures résultant du tri de la table par **id**.

Si `optimization_strategy` contient une chaîne comme **zorder(col1, col2)**, alors l'action diagnose vérifiera que les fichiers de données de la partition sont triés selon **zorder(col1, col2)**.

Le facteur de santé est écrit dans la colonne `corr`. Une valeur de 1.0 signifie que la partition est triée à 100 % selon la stratégie d'optimisation.

| Nom de colonne                        | Description |
| ------------------------------------- | ----------- |
| start_time                            |             |
| full_name string                      |             |
| catalog string                        |             |
| schema string                         |             |
| table_name string                     |             |
| partition_desc                        |             |
| partition_age                         |             |
| n_files_before                        |             |
| n_files_after                         |             |
| num_files_targetted_for_rewrite_before |             |
| num_files_targetted_for_rewrite_after |             |
| n_records_before                      |             |
| n_records_after                       |             |
| avg_file_size_in_mb_before            |             |
| avg_file_size_in_mb_after             |             |
| min_file_size_in_mb_before            |             |
| min_file_size_in_mb_after             |             |
| max_file_size_in_mb_before            |             |
| max_file_size_in_mb_after             |             |
| sum_file_size_in_mb_before            |             |
| sum_file_size_in_mb_after             |             |
| corr_before                           |             |
| corr_after double                     |             |

_Tableau 3 : table partition_health_

## L'Action Diagnosis

Dans le cadre du processus d'optimisation, Ice-Keeper exécute d'abord un diagnostic sur la table pour identifier les partitions nécessitant une optimisation. Vous pouvez invoquer manuellement l'action diagnosis sur une table, même si elle n'est pas encore configurée pour être optimisée. Cela est utile pour vérifier si la table bénéficierait d'une maintenance par Ice-Keeper.

Voici un exemple d'exécution de l'action diagnosis :

```bash
ICEKEEPER_CONFIG=./config/ice-keeper.yaml \
  ./ice-keeper diagnose \
  --full_name dev_catalog.schema1.table1 \
  --max_age_to_diagnose 1000 \
  --optimization_strategy 'address ASC NULLS FIRST, id DESC'
```

La stratégie d'optimisation accepte les valeurs définies dans la propriété tblproperty `ice-keeper.optimization-strategy`.

## Conseils pour les Scripts Bash

Le CLI ice-keeper accepte un argument `--where`. Cela donne à l'administrateur beaucoup de flexibilité pour choisir les tables affectées par la commande émise à ice-keeper. Cependant, rédiger un filtre complexe sur une seule ligne peut être fastidieux. Pour plus de clarté, vous pouvez utiliser des filtres sur plusieurs lignes. Voici comment vous pouvez y parvenir dans un script bash.

Cela définit la variable `where` avec le contenu d'une chaîne multi-lignes, similaire à une chaîne triple guillemets en Python.

```bash
read -r -d '' where << EOM
    catalog in ('dev_catalog')
    and (
      schema like 'xyz%'
      or
      schema like 'abc%'
   )
EOM
```

Vous pouvez ensuite utiliser cette chaîne comme une variable normale.

```bash
./ice-keeper rewrite_manifest \
    --concurrency 32 \
    --spark_driver_cores 8 \
    --spark_driver_memory 8g \
    --spark_executors 5 \
    --spark_executor_cores 10 \
    --spark_executor_memory 10g \
    --where "$where"
```

## Allocation des Ressources Spark

L'action expire exécute à la fois les procédures expire_snapshots et rewrite_manifests. Ces deux procédures n'utilisent pas les workers Spark. Ainsi, nous configurons ice-keeper pour s'exécuter avec `--spark_executors 0`. Cependant, `rewrite_manifests` peut nécessiter beaucoup de mémoire sur certaines tables, nous l'exécutons donc avec une grande quantité de RAM `--spark_driver_memory 32g`.

```bash
./ice-keeper rewrite_manifest \
    --concurrency 32 \
    --spark_driver_cores 8 \
    --spark_driver_memory 8g \
    --spark_executors 5 \
    --spark_executor_cores 10 \
    --spark_executor_memory 10g
```


```bash
./ice-keeper expire \
    --concurrency 32 \
    --spark_driver_cores 16 \
    --spark_driver_memory 16g \
    --spark_executors 10 \
    --spark_executor_cores 10 \
    --spark_executor_memory 10g \
    --where "$where"
```

L'action orphan exécute une procédure `remove_orphan_files` qui s'exécute sur les workers Spark. Cette procédure construit une liste de fichiers existants (côté droit) et une liste de fichiers suivis (côté gauche). Elle joint ensuite ces deux tables pour trouver les fichiers non suivis et les supprime. Comme tout ce travail est effectué sur les workers Spark, nous pouvons exécuter cette procédure sur des centaines de tables simultanément.

```bash
./ice-keeper orphan \
    --concurrency 8 \
    --spark_driver_cores 16 \
    --spark_driver_memory 32g \
    --spark_executors 10 \
    --spark_executor_cores 10 \
    --spark_executor_memory 10g \
    --where "$where"
```

## Développement

### Exécution des tests

Les tests se trouvent dans `ice_keeper_unit_tests.py`. Cette commande exécute les tests unitaires. Elle définit la variable d'environnement ICEKEEPER_CONFIG pour que le fichier de configuration `./tests/config/ice-keeper.yaml` soit utilisé.

```bash
ICEKEEPER_CONFIG=./tests/config/ python -m tests > test.log 2>&1
```

Cette commande exécute un test d'intégration :

```bash
ICEKEEPER_CONFIG=./tests/config/ python -m tests --integration > test.log 2>&1
```

## Plans d'Exécution et Allocations de Ressources

### Expire

La procédure expire_snapshots lit les métadonnées via all_manifests (voir BatchScan(5 et 17)). Elle agrège ensuite et fusionne ces listes.

Une fois cela fait, le driver utilise un `toLocalIterator` et semble supprimer les instantanés en fonction de cet itérateur. Lorsque le driver appelle `toLocalIterator`, les workers utilisent tous leurs CPU pour exécuter ce plan (50 CPU sont utilisés). Du côté du driver, les suppressions sont effectuées à l'aide de threads et peuvent utiliser jusqu'à 30 CPU.

Il utilise un `broadcast hash join`, mais j'ai rencontré cette erreur :

```
Job aborted due to stage failure: Total size of serialized results of 468 tasks (4.0 GiB) is bigger than spark.driver.maxResultSize (4.0 GiB)
```

Pour résoudre ce problème, j'ai configuré le driver pour autoriser des résultats jusqu'à 4 Go. J'ai également augmenté les partitions de shuffle de 200 à 800. Cela crée des tâches plus petites, car les ensembles de données sont divisés en 800 tâches au lieu de 200.

```bash
.config("spark.sql.shuffle.partitions", "800")
.config("spark.driver.maxResultSize", "4g")
```

Voici le plan d'exécution de la procédure expire_snapshots :

```
+- == Current Plan ==
   HashAggregate (25)
   +- Exchange (24)
      +- HashAggregate (23)
         +- BroadcastHashJoin LeftAnti BuildRight (22)
            :- SerializeFromObject (4)
            :  +- MapPartitions (3)
            :     +- DeserializeToObject (2)
            :        +- LocalTableScan (1)
            +- BroadcastExchange (21)
               +- Union (20)
                  :- SerializeFromObject (16)
                  :  +- MapPartitions (15)
                  :     +- DeserializeToObject (14)
                  :        +- ShuffleQueryStage (13)
                  :           +- Exchange (12)
                  :              +- * HashAggregate (11)
                  :                 +- AQEShuffleRead (10)
                  :                    +- ShuffleQueryStage (9), Statistics(sizeInBytes=177.3 MiB, rowCount=7.49E+5)
                  :                       +- Exchange (8)
                  :                          +- * HashAggregate (7)
                  :                             +- * Project (6)
                  :                                +- BatchScan abfss://warehouse@mydatalake.dfs.core.windows.net/iceberg/schema1/telemetry/metadata/320826-59c74ee4-e849-4067-9b91-6fbe5f249b32.metadata.json#all_manifests (5)
                  :- Project (18)
                  :  +- BatchScan abfss://warehouse@mydatalake.dfs.core.windows.net/iceberg/schema1/telemetry/metadata/320826-59c74ee4-e849-4067-9b91-6fbe5f249b32.metadata.json#all_manifests (17)
                  +- LocalTableScan (19)
```

### rewrite_manifest

`LocalTableScan` (voir 4 ci-dessous) semble être la liste des fichiers manifest dans une table de streaming (7 jours d'instantanés), qui peut contenir des milliers de fichiers d'une taille de 10 Mo. `BatchScan` (voir 1 ci-dessous) lit `dev_catalog.schema1.table1.entries` (entrées actuelles, pas sur all_entries). Le nombre d'entrées dans une table peut devenir très élevé, en particulier lorsque la table est écrite à l'aide d'un job Spark streaming. Cela peut atteindre des centaines de milliers (pour une table avec 6 mois de rétention).

En général, ce n'est pas un gros job Spark, mais il peut tout de même bénéficier d'une exécution sur le cluster Spark.

Le plan d'exécution d'une procédure rewrite_manifest :

```
AdaptiveSparkPlan (24)
+- == Final Plan ==
   * SerializeFromObject (14)
   +- MapPartitions (13)
      +- DeserializeToObject (12)
         +- * Sort (11)
            +- ShuffleQueryStage (10), Statistics(sizeInBytes=19.7 MiB, rowCount=5.70E+3)
               +- Exchange (9)
                  +- * Project (8)
                     +- * BroadcastHashJoin LeftSemi BuildRight (7)
                        :- * Project (3)
                        :  +- * Filter (2)
                        :     +- BatchScan dev_catalog.schema1.sa_beacon.entries (1)
                        +- BroadcastQueryStage (6), Statistics(sizeInBytes=8.0 MiB, rowCount=373)
                           +- BroadcastExchange (5)
                              +- LocalTableScan (4)
```

### Orphans

La procédure remove_orphan_files construit une liste de fichiers existants (côté droit). Elle construit également une liste de fichiers suivis (côté gauche). Ces tables sont ensuite jointes pour trouver les fichiers non suivis.

Ce job peut échouer en raison d'une jointure broadcast trop grande. J'ai modifié la configuration Spark pour désactiver la jointure broadcast et privilégier la jointure sort merge, qui ne peut jamais échouer. Il y a probablement un très faible coût à utiliser une jointure sort merge pour les petites tables, mais c'est un petit prix à payer pour la stabilité sur toutes les tables. La tâche ExpireSnapshotTask configure cela avec :

```python
self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

Une fois la liste des fichiers non suivis déterminée, ils sont supprimés, probablement par les workers (à confirmer). Comme tout ce travail est effectué par les workers, nous pouvons exécuter cette procédure sur des centaines de tables simultanément.

Il semble y avoir une première phase où la procédure lit les fichiers de métadonnées, mais sans utiliser une tâche Spark. Cela signifie que je la vois s'exécuter dans l'interface utilisateur Spark, mais je ne vois pas de tâches associées, ce qui rend difficile l'évaluation de l'utilisation des CPU.

Plan d'exécution d'une procédure remove_orphan_files :

```
AdaptiveSparkPlan (86)
+- == Final Plan ==
   * SerializeFromObject (51)
   +- MapPartitions (50)
      +- DeserializeToObject (49)
         +- * SortMergeJoin LeftOuter (48)
            :- * Sort (8)
            :  +- AQEShuffleRead (7)
            :     +- ShuffleQueryStage (6), Statistics(sizeInBytes=223.8 KiB, rowCount=578)
            :        +- Exchange (5)
            :           +- * Project (4)
            :              +- * SerializeFromObject (3)
            :                 +- MapPartitions (2)
            :                    +- Scan (1)
            +- * Sort (47)
               +- AQEShuffleRead (46)
                  +- ShuffleQueryStage (45), Statistics(sizeInBytes=583.9 KiB, rowCount=1.56E+3)
                     +- Exchange (44)
                        +- Union (43)
                           :- * Project (23)
                           :  +- * Filter (22)
                           :     +- * SerializeFromObject (21)
                           :        +- MapPartitions (20)
                           :           +- MapPartitions (19)
                           :              +- DeserializeToObject (18)
                           :                 +- ShuffleQueryStage (17), Statistics(sizeInBytes=44.3 KiB, rowCount=227)
                           :                    +- Exchange (16)
                           :                       +- * HashAggregate (15)
                           :                          +- AQEShuffleRead (14)
                           :                             +- ShuffleQueryStage (13), Statistics(sizeInBytes=53.2 KiB, rowCount=227)
                           :                                +- Exchange (12)
                           :                                   +- * HashAggregate (11)
                           :                                      +- * Project (10)
                           :                                         +- BatchScan dev_catalog.schema1.telemetry1.all_manifests (9)
                           :- * Project (30)
                           :  +- * Filter (29)
                           :     +- * SerializeFromObject (28)
                           :        +- MapPartitions (27)
                           :           +- DeserializeToObject (26)
                           :              +- * Project (25)
                           :                 +- BatchScan dev_catalog.schema1.telemetry1.all_manifests (24)
                           :- * Project (36)
                           :  +- * Filter (35)
                           :     +- * SerializeFromObject (34)
                           :        +- MapPartitions (33)
                           :           +- DeserializeToObject (32)
                           :              +- LocalTableScan (31)
                           +- * Project (42)
                              +- * Filter (41)
                                 +- * SerializeFromObject (40)
                                    +- MapPartitions (39)
                                       +- DeserializeToObject (38)
                                          +- LocalTableScan (37)
```

## Configuration de l'Environnement de Développement

Pour configurer votre environnement de développement dans code-server, exécutez les commandes suivantes.

Cela installera `uv` dans un environnement virtuel privé situé dans `~/.local` et synchronisera les dépendances du projet.

```bash
make install
```

Vous pouvez exécuter les tests en ligne de commande avec :

```bash
make unit-test
```

Pour analyser le projet avec un linter, exécutez :

```bash
make lint
```

Pour configurer votre IDE code-server avec les extensions appropriées :

Tout d'abord, désactivez toutes les extensions :
![alt text](./disable_all_extensions.png "disable_all_extensions")

Ensuite, trouvez les extensions recommandées pour le projet :
![alt text](./find_recommended_extensions.png "find_recommended_extensions")

Notez que vous devrez peut-être exécuter la recherche plusieurs fois avant que code-server ne trouve les extensions recommandées.

Enfin, vous pouvez activer les extensions recommandées pour votre espace de travail :

![alt text](./enable_for_your_workspace.png "enable_for_your_workspace")

## Améliorations Possibles

- [ ] Utiliser la taille des partitions en octets pour estimer une bonne taille cible de fichier pour binpack.
- [ ] Utiliser le nombre de fichiers pour estimer une bonne taille cible de fichier pour les tris zorder, ou peut-être que cela devrait également être basé sur la taille des partitions. Un problème avec zorder est qu'avec un faible nombre de fichiers dans une partition, il est difficile de déterminer précisément si elle est ou non z-ordonnée. En ayant une taille dynamique des fichiers basée sur chaque partition, nous pourrions nous assurer d'avoir un nombre minimum de fichiers par partition et obtenir une meilleure évaluation de la qualité du z-order d'une partition.