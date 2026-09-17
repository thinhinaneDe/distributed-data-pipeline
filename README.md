# Pipeline de données distribuées (PySpark)

Pipeline PySpark sur 2,7 M d'événements GDELT (1,1 Go) : ingestion typée,
nettoyage, deux axes d'agrégation avec jointures de dimension, et un
benchmark pandas vs Spark qui situe le seuil de bascule à ~36 % du corpus.

**Prérequis :** Python 3.12+, JDK 21, ~3 Go d'espace disque, 8 Go de RAM.
Testé sous WSL2 (Ubuntu). Sous Windows natif, Spark nécessite
`winutils.exe` et `hadoop.dll` dans `HADOOP_HOME`.

## 1. Contexte et question traitée

Projet technique de M1 Informatique : construire un pipeline batch
reproductible sur un dataset public volumineux (ingestion, nettoyage,
agrégations, jointure de dimension, écriture Parquet partitionnée), puis
répondre à une question précise plutôt que de se contenter de faire
tourner Spark parce que c'est le sujet : **à partir de quelle volumétrie
un moteur distribué devient-il réellement plus rapide qu'un outil
mono-machine (pandas) sur la même machine, et pourquoi ?**

Le dataset (GDELT 2.0, événements géopolitiques quotidiens) sert de
terrain pour deux décisions d'ingénierie concrètes : une jointure de
dimension avec un choix de stratégie
explicite (broadcast) et sa justification mesurée, et une normalisation
de référentiel face à des incohérences de codes pays réelles, découvertes
en cours de route plutôt que supposées.

## 2. Données

- **Source :** [GDELT 2.0 Event Database](https://www.gdeltproject.org/),
  flux `gdeltv2`, exports au format `.export.CSV.zip` par tranche de
  15 minutes.
- **Volumétrie :** fenêtre du 1er au 30 août 2026 (30 jours × 96 tranches
  = 2880 fichiers). 1,107 Go décompressés, 2 710 905 lignes brutes. Après
  le filtre de fenêtre temporelle de `clean.py` (voir `report/mesures.md`,
  section Ingestion brute) : 2 675 819 événements retenus, écrits en
  Parquet consolidé (39,5 Mo, ~28x plus compact que le CSV brut — voir
  section 4).
- **Schéma :** 61 colonnes tab-delimited, sans en-tête, schéma explicite
  dans `src/schema.py` (validation `FAILFAST`, pas d'inférence de type
  Spark — voir la docstring du fichier pour le raisonnement colonne par
  colonne). Une douzaine de colonnes (identifiants, dates, codes
  d'acteurs et de lieu, mesures d'intensité de l'événement) sont
  exploitées par le pipeline ; une vingtaine d'autres (sous-catégories
  ethniques/religieuses des acteurs) sont vides à 97-100 % sur
  l'échantillon mesuré et écartées dès `clean.py`.
- **Comment les obtenir :** les événements bruts ne sont pas versionnés
  (`data/` gitignoré). `src/download_gdelt.py` télécharge et décompresse
  la fenêtre choisie :
  ```bash
  python src/download_gdelt.py --start 20260801 --end 20260830
  ```
  Exception : `data/reference/FIPS.country.txt` et `CAMEO.country.txt`
  (tables de correspondance code → nom de pays utilisées par
  `transform.py`, ~8 Ko à elles deux) sont versionnées malgré la règle
  générale — aucun script ne les télécharge, et sans elles `make
  aggregate` échoue sur une machine vierge. Récupérées depuis
  [gdeltproject.org/data/lookups](https://www.gdeltproject.org/data/lookups/)
  le 17 septembre 2026, figées à cette date : si GDELT met ces tables à
  jour, le dépôt ne suit pas automatiquement (voir section 6).

## 3. Méthode

1. **Téléchargement** (`src/download_gdelt.py`) : récupère les tranches
   de 15 min sur la fenêtre demandée, idempotent (fichier déjà présent =
   ignoré).
2. **Ingestion** (`src/ingest.py`) : lecture avec schéma explicite et
   mode `FAILFAST`, pas de sélection ni de filtre — valide seulement que
   les données brutes correspondent au schéma déclaré.
3. **Nettoyage** (`src/clean.py`) : projection à 12 colonnes utiles avant
   tout comptage (pas de cache — voir section 5), filtre de fenêtre
   temporelle sur `SQLDATE` (35 086 lignes exclues, comptées et
   affichées, jamais silencieuses), écriture en Parquet partitionné par
   `SQLDATE`.
4. **Agrégation** (`src/transform.py`) : deux axes, chacun avec une
   jointure de dimension en broadcast hash join explicite (voir
   section 5) :
   - Axe 1 — couverture par pays d'action : `ActionGeo_CountryCode`
     (FIPS 10-4) rapproché de `data/reference/FIPS.country.txt`, après
     normalisation de deux codes FIPS alternatifs (`RB`, `YI` → `RI`
     pour la Serbie — voir `src/fips_country_aliases.py`) qui, non
     corrigés, faisaient perdre silencieusement 1 521 événements à la
     jointure interne. Agrégation par pays et par jour : nombre
     d'événements, tonalité moyenne, répartition par `QuadClass`.
   - Axe 2 — interactions entre pays acteurs : `Actor1CountryCode` et
     `Actor2CountryCode` (CAMEO) rapprochés de la classification
     manuelle de `src/cameo_country_types.py` (pays / région /
     territoire), filtrés à `entity_type == "pays"` des deux côtés
     (21,4 % conservés, taux affiché, pas appliqué en silence). Les
     paires domestiques (`Actor1CountryCode == Actor2CountryCode`) sont
     ensuite exclues de l'analyse des interactions entre pays
     différents, qui porte donc sur environ 15,6 % du corpus initial
     une fois les deux filtres combinés.
5. **Benchmark** (`src/benchmark.py`) : cinq comparaisons mesurées et
   consignées dans `report/mesures.md` — voir sections 4 et 5.

Chaque règle de nettoyage, chaque choix de jointure et chaque trou de
couverture de référentiel rencontrés en cours de route sont mesurés et
documentés dans `report/mesures.md`, avec la commande exacte qui a
produit chaque chiffre.

## 4. Résultats chiffrés

### Le benchmark central : pandas vs PySpark, même agrégation, plusieurs volumétries

Comptage d'événements par `ActionGeo_CountryCode` sur les CSV bruts,
identique des deux côtés, mesuré dans des sous-processus isolés (JVM
froide côté Spark, pic mémoire propre côté pandas — voir
`src/spark_benchmark_worker.py` et `src/pandas_benchmark_worker.py`) :

| Volumétrie | Fichiers | Spark (froid) | pandas | Pic mémoire pandas |
|---|---|---|---|---|
| 1 % | 29 | 18,28 s | 0,88 s | 143,9 Mo |
| 10 % | 288 | 23,93 s | 8,82 s | 389,1 Mo |
| 25 % | 720 | 28,22 s | 21,89 s | 943,6 Mo |
| 35 % | 1008 | 27,79 s | 26,45 s | 1 226,9 Mo |
| 45 % | 1296 | 30,10 s | 34,92 s | 1 614,3 Mo |
| 100 % | 2880 | 48,60 s | 83,33 s | 3 430,0 Mo |

**Seuil de croisement mesuré : entre 35 % et 45 % du corpus** (~36 % par
interpolation linéaire entre ces deux points — voir `report/mesures.md`,
section Comparaison 5, pour le détail et l'historique de la mesure,
d'abord estimée par régression sur 3 points puis resserrée par mesure
directe). En dessous, pandas gagne ; au-dessus, Spark gagne. pandas n'a
échoué à aucune volumétrie, y compris 100 % (3,43 Go de pic sur une
machine à 7,6 Go de RAM).

### Résultats à l'appui

| Mesure | Résultat |
|---|---|
| Taille sur disque, CSV brut vs Parquet consolidé | 1,107 Go → 39,5 Mo (~28,1x) |
| Broadcast hash join explicite vs sort-merge forcé (même jointure) | ~4,8 s / 143 tâches vs ~5,4 s / 228 tâches, en régime stable |
| Normalisation FIPS (RB, YI → RI), axe 1 | 1 420 → 2 941 événements Serbie (+1 521) |
| Filtre `entity_type == "pays"`, axe 2 | 572 425 conservés sur 2 675 819 (21,4 %) |
| Paires domestiques (Actor1 == Actor2), axe 2 | 27,2 % des événements du corpus filtré, 186 paires sur 9595 |
| Effet week-end | 102 313 événements le mardi 4 août contre 54 796 le dimanche 9 |
| Rétrospection médiatique | 17 213 événements datés d'un an plus tôt, ~700/jour, uniformément répartis |

Détail complet, commandes exactes et repérage des trois exécutions par
mesure (là où c'est fait) : `report/mesures.md`.

### Ce que produit le pipeline

Sortie réelle de `python src/transform.py`, lue depuis
`data/processed/aggregates/` :

Axe 1 — les 5 pays les plus couverts (nombre d'événements, tous jours
cumulés) :

| Code | Pays | Événements |
|---|---|---|
| US | United States | 837 338 |
| IN | India | 164 363 |
| UK | United Kingdom | 142 491 |
| NI | Nigeria | 110 409 |
| IS | Israel | 97 936 |

Axe 2 — les 5 paires d'acteurs les plus fréquentes :

| Actor1 | Actor2 | Événements | Tonalité moyenne |
|---|---|---|---|
| USA | USA | 64 656 | -2,472 |
| USA | IRN | 8 928 | -3,458 |
| RUS | UKR | 8 707 | -5,135 |
| UKR | RUS | 8 429 | -4,959 |
| IRN | USA | 8 261 | -3,547 |

## 5. Analyse

**Pourquoi pandas gagne aux petites volumétries.** Le temps Spark croît
très peu avec le volume (18,28 s → 23,93 s → 48,60 s entre 1 % et 100 %) :
la majeure partie de son temps aux petites volumétries est un coût fixe
de démarrage de la JVM et d'initialisation de la session (planification,
codegen Catalyst), payé une fois par run froid, indépendamment de la
quantité de données. pandas n'a pas ce coût de démarrage : son temps
croît presque proportionnellement au volume (0,88 s → 83,33 s, quasi ×10
à chaque ×10 de données), sans plafond fixe pour l'amortir. En dessous du
seuil mesuré (35-45 %), le volume de données ne suffit pas à rentabiliser
le coût de démarrage de Spark ; au-dessus, l'absence de parallélisation
côté pandas devient le facteur dominant.

**Pourquoi le pic mémoire pandas ne scale pas linéairement.** Une
extrapolation naïve du pic à 1 % (143,9 Mo) donnait ~14 Go à 100 %, ce
qui aurait dépassé la RAM disponible. Mesuré : 3,43 Go, largement en
dessous. Une partie du coût mémoire de pandas est un plancher fixe par
lecture (objets DataFrame, structures d'index, overhead par colonne),
pas strictement proportionnel au nombre de lignes — un piège classique
d'extrapolation à partir d'un seul petit échantillon, corrigé ici par la
mesure plutôt que par la supposition.

**Pourquoi Parquet compresse ~28x.** Stockage colonnaire (types homogènes
par colonne, meilleure compressibilité qu'un texte tabulé ligne par
ligne) et projection à 12 colonnes sur les 61 d'origine dès l'écriture
(voir `clean.py`) : les deux effets se cumulent, pas un seul.

**Pourquoi le broadcast hash join gagne, mais modestement en local[*].**
Les deux tables de référence (FIPS, CAMEO) font 262-274 lignes contre
2,7 M côté événements : les broadcaster évite un stage de shuffle entier
(`Exchange hashpartitioning`, visible dans `.explain()` du plan
sort-merge forcé) — 85 tâches de moins mesurées. L'écart de temps reste
modeste (~10-15 %) parce qu'en `local[*]`, ce shuffle est un échange sur
disque local dans la même JVM, pas un aller-retour réseau entre
exécuteurs ; sur un vrai cluster, le même plan sort-merge impliquerait
sérialisation et transfert réseau, et l'écart se creuserait nettement
plus. Le choix explicite reste justifié indépendamment du gain de temps
local : Spark aurait de toute façon broadcasté ces tables automatiquement
(bien en dessous du seuil de 10 Mo), mais écrire le choix dans le code
le rend visible et le protège d'un futur seuil de configuration modifié
ailleurs dans le projet.

**Pourquoi les référentiels FIPS et CAMEO ont des trous.** Deux natures
de trou différentes, pas une seule : SSD (Soudan du Sud) manque
entièrement de CAMEO.country.txt — référentiel jamais mis à jour depuis
l'indépendance de 2011. RB et YI ne manquent pas au FIPS 10-4 en
général (RB est le code FIPS actuel de la Serbie) — c'est notre
`FIPS.country.txt` qui n'a qu'une entrée par pays (RI) quand GDELT en
émet plusieurs pour le même pays selon l'ancienneté du géocodage. Les
deux corrections (ajout pour SSD, normalisation pour RB/YI) sont écrites
en code, pas dans les fichiers de référence téléchargés — voir section 6
pour la limite structurelle de la méthode de détection utilisée.

## 6. Limites et pistes d'amélioration

- **Pas de cluster réel testé.** Toutes les mesures sont en `local[*]`
  sur une seule machine. Le gain du broadcast explicite (section 5)
  serait plus marqué sur un vrai cluster (shuffle réseau, pas disque
  local) ; le seuil pandas/Spark mesuré ici (35-45 %) ne se transpose pas
  tel quel à un cluster, où le coût de démarrage de Spark serait
  différent (plus élevé pour l'allocation des ressources, potentiellement
  amorti différemment si la session reste chaude entre plusieurs jobs).
- **Détection des trous de référentiel non exhaustive.** RB et YI ont été
  trouvés parce qu'ils ne matchaient AUCUNE entrée de `FIPS.country.txt`
  (voir `report/mesures.md`, section Limite de cette détection). Un pays
  émettant deux codes qui matchent chacun une entrée DIFFÉRENTE du
  référentiel passerait inaperçu et serait compté deux fois dans les
  agrégations, sans qu'aucune mesure de ce pipeline ne le révèle. Seule
  une revue manuelle du référentiel (chercher des noms en double, pas des
  codes en double) le détecterait — non faite ici.
- **Comparaison pandas vs Spark en lecture pleine largeur, pas en usage
  réaliste pandas.** Les deux moteurs lisent les 61 colonnes typées pour
  rester strictement comparables (même coût de parsing des deux côtés).
  Un usage pandas réaliste utiliserait `usecols` pour ne lire que les 2-3
  colonnes nécessaires à l'agrégation, ce qui accélérerait pandas
  au-delà des chiffres mesurés ici — la comparaison mesure un coût de
  lecture équivalent, pas la meilleure stratégie pandas possible pour
  cette tâche précise.
- **Comparaison 5 mesurée une seule fois par volumétrie**, contrairement
  aux comparaisons 1, 3 et 4 (3 exécutions). Plus bruitée, signalé
  explicitement dans `report/mesures.md`.
- **Fenêtre d'un mois, pas d'effet saisonnier contrôlé.** L'effet
  week-end est mesuré (voir Ingestion brute) mais rien ne corrige la
  volumétrie ou les agrégations pour ça ; un mois différent de l'année
  pourrait déplacer le seuil pandas/Spark en changeant simplement le
  volume quotidien moyen.
- **À 10x cette volumétrie**, `spark.sql.shuffle.partitions` (200 par
  défaut, jamais réglé ici) produirait probablement un sur-découpage en
  petites tâches en `local[*]` — non mesuré, mais visible en germe dans
  le nombre de tâches du plan sort-merge de la comparaison 4. pandas,
  lui, buterait presque certainement sur la mémoire disponible (la
  non-linéarité mesurée en section 5 ne garantit rien au-delà de 100 %
  du corpus actuel).
- **Tables de référence figées, pas suivies.**
  `data/reference/FIPS.country.txt` et `CAMEO.country.txt` sont
  versionnées telles que récupérées le 17 septembre 2026 (voir
  section 2). Si GDELT les met à jour (nouveau pays, code corrigé), le
  dépôt ne le reflète pas automatiquement — il faudrait retélécharger et
  recommitter manuellement, avec le risque de retomber sur les mêmes
  trous de couverture (SSD, RB/YI) sous une autre forme.
- **Pas de suite de tests automatisée.** `tests/smoke_test_spark.py` et
  `tests/check_schema.py` sont des scripts de vérification ponctuelle
  (plomberie Java/PySpark, complétude du schéma), pas des tests
  répétables en CI.

## 7. Reproduction

```bash
git clone <url-du-dépôt>
cd distributed-data-pipeline

make venv                                                # crée le venv, installe requirements.txt figé
python src/download_gdelt.py --start 20260801 --end 20260830   # télécharge les données brutes (data/, gitignoré)

make ingest     # valide le schéma des CSV bruts (FAILFAST)
make clean      # nettoie, projette, écrit le Parquet consolidé
make aggregate  # agrégations des deux axes, jointures broadcast
make bench      # les 5 comparaisons de benchmark (voir report/mesures.md)
```

`tests/smoke_test_spark.py` est un test de plomberie (pas le pipeline) :
il vérifie que la chaîne Java/PySpark/Parquet fonctionne. À lancer avec
`python tests/smoke_test_spark.py` après `make venv`.

`tests/check_schema.py <motif>` vérifie que `GDELT_EVENTS_SCHEMA` lit un
sous-ensemble de fichiers réels sans erreur FAILFAST et affiche le taux
de null par colonne, ex. :
```bash
python tests/check_schema.py 'data/raw/20260804*.export.CSV'
```

Détail de chaque mesure citée dans ce README (commande exacte, date,
contexte) : `report/mesures.md`.
