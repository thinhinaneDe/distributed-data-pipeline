# Journal de mesures

Ce fichier ne contient que du mesuré, jamais de l'estimé. Chaque mesure est
datée et accompagnée de la commande exacte qui l'a produite, pour rester
reproductible. Une section par étape du projet, enrichie au fil de l'avancement.

## Ingestion brute — 15 septembre 2026

Source : GDELT 2.0, flux `/gdeltv2/`, exports 15 min.
Fenêtre : 1er au 30 août 2026.

### Complétude du téléchargement

2880 fichiers récupérés sur 2880 tranches de 15 min attendues (30 jours × 96),
aucun trou sur cette fenêtre.

```bash
python src/download_gdelt.py --start 20260801 --end 20260830
ls data/raw | wc -l
```
→ `2880`

### Volume décompressé

```bash
du -sb data/raw | cut -f1
```
→ `1107421246` octets = **1,107 Go**

Taille moyenne par fichier (calcul, pas une commande séparée) :
1 107 421 246 / 2880 ≈ **384 ko**.

### Nombre de colonnes

```bash
awk -F'\t' '{print NF; exit}' data/raw/20260801000000.export.CSV
```
→ `61` — cohérent avec le codebook GDELT Events V2.0.

### Volume d'événements

```bash
cat data/raw/*.export.CSV | wc -l
```
→ **2 710 905** événements sur 30 jours, soit ~90 363/jour (2 710 905 / 30, calcul).

**Hypothèse implicite de cette commande, à garder en tête** : `wc -l` compte
des lignes, pas des événements — l'équivalence ligne = événement suppose
qu'aucun champ ne contient de retour à la ligne. Vrai ici : les fichiers
GDELT sont tab-delimited sans champ texte long ni guillemets d'échappement
(contrairement à du CSV RFC 4180 classique), donc pas de retour à la ligne
interne aux champs. À revérifier si un jour on mélange cette source avec un
format qui, lui, peut en contenir.

### Effet week-end

```bash
cat data/raw/20260801*.export.CSV | wc -l          # 1er août = samedi
```
→ `68900` événements, contre ~90 363/jour en moyenne.

Volumes en octets, semaine vs week-end :

```bash
du -cb data/raw/20260804*.export.CSV | tail -1 | cut -f1   # mardi 4 août (semaine, 1 jour)
du -cb data/raw/2026080[89]*.export.CSV | tail -1 | cut -f1        # we 8-9 août (2 jours cumulés)
du -cb data/raw/202608{15,16}*.export.CSV | tail -1 | cut -f1      # we 15-16 août (2 jours cumulés)
du -cb data/raw/202608{22,23}*.export.CSV | tail -1 | cut -f1      # we 22-23 août (2 jours cumulés)
```

**Attention à l'unité** : les trois commandes de week-end portent chacune
sur deux fichiers-jours (samedi+dimanche), donc sur 48h — le total brut
n'est pas comparable tel quel au chiffre du mardi, qui porte sur 24h. Valeurs
divisées par 2 ci-dessous pour ramener à un volume par jour :

| Période | Total brut (octets, 48h) | Ramené au jour |
|---|---|---|
| 8-9 août | 48 933 057 | 24 466 529 (~24,5 Mo/jour) |
| 15-16 août | 49 539 416 | 24 769 708 (~24,8 Mo/jour) |
| 22-23 août | 49 037 736 | 24 518 868 (~24,5 Mo/jour) |

Comparé au mardi 4 août (24h) : `41952905` octets (~42,0 Mo/jour).
Même unité des deux côtés : ~24,5-24,8 Mo/jour de week-end contre ~42,0
Mo/jour de semaine.

Même comparaison en nombre d'événements (plus parlant que les octets) :

```bash
for d in 20260804 20260808 20260809; do
  echo -n "$d : "; cat data/raw/$d*.export.CSV | wc -l
done
```

| Jour | Événements | % du mardi |
|---|---|---|
| mardi 4 août (semaine) | 102 313 | 100 % |
| samedi 8 août | 65 273 | 63,8 % |
| dimanche 9 août | 54 796 | 53,6 % |

Le dimanche est plus creux que le samedi — un gradient que la mesure en
octets (qui regroupait les deux jours de week-end ensemble) ne faisait pas
apparaître.

Interprétation : GDELT mesure la couverture de presse mondiale, pas
l'activité du monde — la baisse le week-end reflète la baisse de production
journalistique, pas une baisse d'événements réels.

### Distribution des codes pays (Actor1CountryCode, 1er août)

```bash
cut -f8 data/raw/20260801*.export.CSV | sort | uniq -c | sort -rn | head -10
```

| Code | Occurrences |
|---|---|
| (vide) | 30 674 |
| USA | 11 539 |
| GBR | 1 810 |
| ISR | 1 585 |
| IRN | 1 372 |
| NGA | 1 296 |
| ESP | 1 264 |
| IND | 1 145 |
| CHN | 887 |
| RUS | 885 |

**Point ouvert, non résolu ici** : la valeur la plus fréquente
d'`Actor1CountryCode` est l'absence de valeur (30 674 contre 11 539 pour
USA, le premier code non vide). Décision de nettoyage (exclure, imputer,
garder comme catégorie à part entière) à prendre plus tard, par moi.

### Distribution des années (champ SQLDATE, 1er août)

```bash
cut -f2 data/raw/20260801*.export.CSV | cut -c1-4 | sort | uniq -c | sort -rn
```

| Année | Occurrences | % |
|---|---|---|
| 2026 | 68 634 | 99,64 % |
| 2025 | 248 | 0,36 % |
| 2016 | 18 | 0,03 % |

Les 18 lignes de 2016 portent des `GlobalEventID` très bas (ex. 1316310127,
1316310128) :

```bash
awk -F'\t' '$2 ~ /^2016/ {print $1}' data/raw/20260801*.export.CSV | sort -u
```

Interprétation : enregistrements anciens rediffusés dans le flux du jour,
placés en tête de fichier par tri d'ID. Volume négligeable (0,03 %), mais à
traiter par un filtre de fenêtre temporelle documenté plutôt qu'ignoré
silencieusement.

### Lignes exclues par le filtre de fenêtre temporelle — 17 septembre 2026

`clean.py` (voir section Benchmarks) filtre sur `SQLDATE` et affiche
**35 086 lignes supprimées** sur les 2880 fichiers. Ce chiffre n'était pas
expliqué jusqu'ici. Distribution par mois de `SQLDATE` sur l'ensemble des
2880 fichiers (pas seulement le 1er août, contrairement à la section
précédente) :

```bash
cut -f2 data/raw/*.export.CSV | cut -c1-6 | sort | uniq -c | sort -rn
```

| Mois (AAAAMM) | Occurrences | Interprétation |
|---|---|---|
| 202608 | 2 675 819 | fenêtre |
| 202607 | 17 327 | débordement de fenêtre |
| 202508 | 17 213 | rétrospection à un an |
| 201608 | 530 | — |
| 201609 | 16 | — |

Somme des lignes hors fenêtre : 17 327 + 17 213 + 530 + 16 = **35 086**,
identique au compte de `clean.py`. Les lignes 201608/201609 recoupent les
enregistrements de 2016 déjà repérés plus haut sur l'échantillon du 1er
août (`GlobalEventID` très bas, rediffusion d'anciens enregistrements).

Le groupe 202508 (17 213 lignes, le deuxième en volume après la fenêtre
elle-même) mérite un examen séparé : uniforme dans le temps, ou concentré
sur quelques jours de téléchargement (auquel cas ce serait un incident de
flux plutôt qu'un phénomène structurel) ?

```bash
awk -F'\t' '$2 ~ /^202508/ {print substr(FILENAME, 10, 8)}' data/raw/*.export.CSV | sort | uniq -c | sort -rn
```

→ 600 à 850 lignes par jour de téléchargement, sur les 30 jours de la
fenêtre, sans pic isolé : réparti uniformément, donc structurel et non
incident de flux ponctuel.

Interprétation : GDELT date un événement à la date décrite dans l'article
source, pas à la date de publication de cet article. Le flux quotidien
contient donc en permanence des articles rétrospectifs qui renvoient à des
faits antérieurs — ici, majoritairement des faits vieux d'un an (août
2025). C'est une deuxième preuve, après l'effet week-end (voir plus haut),
que ce corpus mesure la couverture de presse et non l'activité du monde :
un article publié aujourd'hui peut décrire un fait ancien, et GDELT
l'indexe à la date du fait, pas à celle de sa mise en couverture.

## Schéma et complétude des colonnes — 15 septembre 2026

Schéma explicite des 61 colonnes défini dans `src/schema.py`, vérifié en
mode `FAILFAST` (Spark lève une exception au premier écart entre les
données et le type déclaré, au lieu de mettre à null en silence comme le
mode `PERMISSIVE` par défaut).

```bash
python tests/check_schema.py 'data/raw/20260804*.export.CSV'
```

Périmètre : mardi 4 août 2026, journée complète (96 fichiers de 15 min).

- Lecture en FAILFAST : aucune exception, donc aucune ligne du 4 août ne
  contredit un type du schéma.
- Lignes lues par Spark : **102 313**. Lignes comptées par `wc -l` sur les
  96 fichiers : **102 313**. Comptes identiques.
- Note d'exécution : un WARN Spark (`FileStreamSink`,
  `FileNotFoundException` sur le chemin littéral contenant `*`) apparaît
  dans les logs — c'est une vérification interne bénigne (Spark teste si le
  chemin est un répertoire de checkpoint de streaming avant de résoudre le
  glob normalement), sans effet sur le résultat ; confirmé par un code de
  sortie 0 et les deux comptes identiques.

Taux de null par colonne, journée complète (96 fichiers, 102 313 lignes) :

| Colonne | Null | % |
|---|---|---|
| Actor2Type3Code | 102 271 | 100,0 % |
| Actor1Type3Code | 102 241 | 99,9 % |
| Actor2Religion2Code | 102 041 | 99,7 % |
| Actor1Religion2Code | 101 989 | 99,7 % |
| Actor2EthnicCode | 101 873 | 99,6 % |
| Actor1EthnicCode | 101 756 | 99,5 % |
| Actor2KnownGroupCode | 101 678 | 99,4 % |
| Actor1KnownGroupCode | 101 435 | 99,1 % |
| Actor2Religion1Code | 101 195 | 98,9 % |
| Actor1Religion1Code | 101 018 | 98,7 % |
| Actor2Type2Code | 100 436 | 98,2 % |
| Actor1Type2Code | 99 754 | 97,5 % |
| Actor2Type1Code | 68 897 | 67,3 % |
| Actor2Geo_ADM2Code | 64 033 | 62,6 % |
| Actor2CountryCode | 58 939 | 57,6 % |
| Actor1Type1Code | 58 291 | 57,0 % |
| ActionGeo_ADM2Code | 47 303 | 46,2 % |
| Actor1CountryCode | 45 912 | 44,9 % |
| Actor1Geo_ADM2Code | 44 850 | 43,8 % |
| Actor2Geo_Fullname | 33 397 | 32,6 % |
| Actor2Geo_Lat | 33 397 | 32,6 % |
| Actor2Geo_Long | 33 391 | 32,6 % |
| Actor2Geo_CountryCode | 33 387 | 32,6 % |
| Actor2Geo_ADM1Code | 33 387 | 32,6 % |
| Actor2Geo_FeatureID | 33 387 | 32,6 % |
| Actor2Code | 31 451 | 30,7 % |
| Actor2Name | 31 451 | 30,7 % |
| Actor1Geo_Fullname | 12 608 | 12,3 % |
| Actor1Geo_Lat | 12 608 | 12,3 % |
| Actor1Geo_Long | 12 598 | 12,3 % |
| Actor1Geo_CountryCode | 12 594 | 12,3 % |
| Actor1Geo_ADM1Code | 12 594 | 12,3 % |
| Actor1Geo_FeatureID | 12 594 | 12,3 % |
| Actor1Code | 9 983 | 9,8 % |
| Actor1Name | 9 983 | 9,8 % |
| ActionGeo_Fullname | 2 896 | 2,8 % |
| ActionGeo_Lat | 2 896 | 2,8 % |
| ActionGeo_Long | 2 883 | 2,8 % |
| ActionGeo_CountryCode | 2 880 | 2,8 % |
| ActionGeo_ADM1Code | 2 880 | 2,8 % |
| ActionGeo_FeatureID | 2 880 | 2,8 % |
| Toutes les autres colonnes (ID, dates, EventCode/Base/Root, QuadClass, Goldstein, Num*, AvgTone, les trois Geo_Type, DATEADDED, SOURCEURL) | 0 | 0,0 % |

Comparaison avec l'échantillon d'une seule tranche de 15 min (1er août,
2 540 lignes, voir plus haut) : les proportions sont globalement stables
d'un fichier à l'échelle d'une journée complète — pas d'effet d'échantillon
visible sur ces taux de null.

## Benchmarks — 16 septembre 2026

Trois questions avant de figer `clean.py`, mesurées via `src/benchmark.py`,
en local[*] sur la même machine, jamais de cluster.

### Comparaison 1 — effet de la consolidation Parquet

Même agrégation (comptage d'événements par `ActionGeo_CountryCode`) sur les
2880 CSV bruts (schéma explicite, FAILFAST) vs le Parquet consolidé. Trois
exécutions par format pour voir la variabilité.

```bash
python src/benchmark.py --comparisons 1
```

| | CSV bruts | Parquet consolidé |
|---|---|---|
| run 1 | 14,77 s / 294 tâches / 97 partitions | 5,56 s / 204 tâches / 67 partitions |
| run 2 | 8,44 s / 294 tâches / 97 partitions | 3,60 s / 204 tâches / 67 partitions |
| run 3 | 8,22 s / 294 tâches / 97 partitions | 3,16 s / 204 tâches / 67 partitions |

249 groupes identiques des deux côtés (contrôle de cohérence entre les deux
sources). Run 1 plus lent que les suivants sur les deux formats : cache
disque de l'OS pas encore chaud au premier passage, sur les deux formats.
Parquet ~2,5x plus rapide et moins de tâches grâce à un fichier source
~28x plus petit (colonnes déjà prunées à l'écriture, pas de reparsing de
texte tabulé à chaque lecture).

### Comparaison 2 — taille sur disque

```bash
python src/benchmark.py --comparisons 2
```

Équivalent interne à `du -sb data/raw` et `du -sb data/processed/events`.

| Format | Taille |
|---|---|
| CSV bruts | 1 107 421 246 octets (~1,11 Go) |
| Parquet consolidé | 39 462 309 octets (~39,5 Mo) |

Rapport ~28,1x : compression colonnaire Parquet + projection à 12 colonnes
sur les 61 d'origine.

### Comparaison 3 — cache vs projection dans `clean.py`

Question initiale : l'ordre de l'ancien `clean.py` (cache → count → select)
coûte-t-il ou aide-t-il ? Un premier essai à deux variantes (cache avant
select sur 61 colonnes vs sans cache après select sur 12 colonnes) confond
deux causes possibles — présence du cache et largeur des données mises en
cache. Ajout d'une troisième variante isolant le cache seul (même largeur,
12 colonnes, des deux côtés) pour trancher.

```bash
python src/benchmark.py --comparisons 3
```

| Variante | run 1 | run 2 | run 3 |
|---|---|---|---|
| cache avant select, 61 colonnes (ancien `clean.py`) | 102,65 s | 68,87 s | 65,00 s |
| select puis cache, 12 colonnes | 25,81 s | 23,11 s | 20,53 s |
| select → count, sans cache | 23,90 s | 17,36 s | 18,07 s |

Taille réelle du bloc caché (`spark.sparkContext._jsc.sc().getRDDStorageInfo()`),
`spark.driver.memory` non fixé explicitement (défaut Spark : 1 Go) :

| Variante | mémoire | disque | |
|---|---|---|---|
| cache avant select, 61 colonnes | 438-447 Mo | 134-142 Mo | **spill disque** |
| cache après select, 12 colonnes | 80 Mo | 0 | tout en mémoire |

Interprétation : la lenteur de l'ancien `clean.py` n'est pas due au cache en
soi, mais au fait de cacher 61 colonnes (~440 Mo) sous un driver à 1 Go par
défaut — ça dépasse la mémoire de stockage disponible et force un spill sur
disque (~140 Mo), plus coûteux qu'un second passage disque profitant du
cache OS déjà chaud. Une fois la projection faite avant le cache (12
colonnes, 80 Mo), tout tient en mémoire et le cache devient quasi gratuit,
proche du temps sans cache — la variante "select puis cache" est proche de
"sans cache", pas de "cache 61 colonnes".

Décision : `clean.py` projette désormais les 12 colonnes avant tout
comptage, sans cache — la variante la plus rapide, la plus simple, et
indépendante d'un réglage de `spark.driver.memory`.

### Comparaison 4 — broadcast explicite vs sort-merge forcé, 17 septembre 2026

Avant d'écrire `transform.py` : les deux jointures de dimension (axe 1
ActionGeo_CountryCode/FIPS, axe 2 Actor1/Actor2CountryCode/CAMEO) portent
sur des tables de 274 et 262 lignes, quelques dizaines de Ko, contre 2,7 M
de lignes côté événements. `transform.py` les broadcast explicitement
plutôt que de laisser Catalyst décider seul via
`spark.sql.autoBroadcastJoinThreshold` (10 Mo par défaut — ces tables
seraient de toute façon broadcastées automatiquement). Mesure ici pour
vérifier que ce choix n'est pas qu'une préférence de lisibilité : même
jointure (Actor1CountryCode -> table CAMEO), broadcast explicite contre
sort-merge forcé (seuil mis à -1 pour désactiver le broadcast automatique
et obtenir un vrai sort-merge, pas la même stratégie mesurée deux fois).

```bash
python src/benchmark.py --comparisons 4
```

Plan (`.explain()`, run 1 de chaque stratégie) : broadcast explicite ->
`BroadcastHashJoin`, aucun shuffle. Sort-merge forcé -> `SortMergeJoin`,
shuffle présent (`Exchange hashpartitioning`).

Broadcast explicite : run 1 11,93 s / 143 tâches, run 2 5,31 s / 143
tâches, run 3 4,29 s / 143 tâches. 1 483 939 lignes appariées, identique
sur les trois runs.

Sort-merge forcé : run 1 7,43 s / 228 tâches, run 2 5,61 s / 228 tâches,
run 3 5,12 s / 228 tâches. Mêmes 1 483 939 lignes appariées.

Run 1 plus lent des deux côtés (JIT/codegen Catalyst pas encore chaud,
comme en comparaison 1) donc peu informatif seul ; en régime stable (runs
2 et 3), broadcast explicite ~4,8 s en moyenne contre ~5,4 s pour
sort-merge forcé, soit un écart modeste (~10-15 %) mais net et
systématique. L'écart de tâches est plus parlant que l'écart de temps :
228 contre 143, soit 85 tâches de shuffle en plus (lecture + écriture
d'échange) pour repartitionner par hachage une table de faits qu'il
n'était pas nécessaire de repartitionner. En local[*], ce shuffle reste
un échange sur disque local dans la même JVM, pas un aller-retour réseau
entre machines — c'est pourquoi l'écart de temps reste modeste ici ; sur
un vrai cluster, le même `Exchange hashpartitioning` impliquerait une
sérialisation et un transfert réseau entre exécuteurs, et l'écart se
creuserait nettement plus.

Décision : broadcast explicite conservé dans `transform.py` pour les deux
axes — gain mesuré et net en tâches (donc en travail réellement effectué
par Spark), gain de temps plus modeste en local[*] mais qui s'aggraverait
sur cluster, et choix qui reste correct même si une table de référence
grossissait au-delà du seuil de 10 Mo, ce que le broadcast automatique
seul ne garantirait pas.

## Typologie des codes acteurs — 17 septembre 2026

CAMEO.country.txt mélange sous un même référentiel de 261 codes à 3
lettres des pays, des zones non nationales (continents, régions
géopolitiques, une ville) et des territoires dépendants ou à
souveraineté disputée. Classification manuelle consignée dans
`src/cameo_country_types.py` (décision de projet, pas une donnée
téléchargée, donc dans src/ et non dans data/) : 193 codes pays, 40
codes territoire, 28 codes region. Raisonnement détaillé dans la
docstring du fichier.

Mesure sur le Parquet consolidé (`data/processed/events`, 2 675 819
événements), répartition d'Actor1CountryCode par catégorie :

```bash
python3 - << 'PY'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from cameo_country_types import CAMEO_COUNTRY_TYPES

spark = SparkSession.builder.master("local[*]").appName("mesure-actor1-types").getOrCreate()
df = spark.read.parquet("data/processed/events")
types_df = spark.createDataFrame(
    [(code, etype) for code, (_, etype) in CAMEO_COUNTRY_TYPES.items()],
    ["code", "entity_type"],
)
joined = df.join(types_df, df.Actor1CountryCode == types_df.code, "left")
(
    joined
    .withColumn("bucket", when(col("Actor1CountryCode").isNull(), "null")
                .otherwise(when(col("entity_type").isNull(), "code_inconnu")
                           .otherwise(col("entity_type"))))
    .groupBy("bucket").count().orderBy(col("count").desc())
    .show(truncate=False)
)
PY
```

pays : 1 420 217 événements, 53,08 %
null (Actor1CountryCode vide) : 1 191 880 événements, 44,54 %
region : 38 207 événements, 1,43 %
territoire : 24 337 événements, 0,91 %
code_inconnu (non nul, absent des 261 codes CAMEO) : 1 178
événements, 0,04 %

Événements avec un Actor1CountryCode de type region ou territoire
plutôt que pays (hors null, hors code_inconnu) : 62 544, soit 2,337 %
du total.

Détail du bucket code_inconnu :

```bash
python3 - << 'PY'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from cameo_country_types import CAMEO_COUNTRY_TYPES

spark = SparkSession.builder.master("local[*]").appName("codes-inconnus").getOrCreate()
df = spark.read.parquet("data/processed/events")
types_df = spark.createDataFrame([(c,) for c in CAMEO_COUNTRY_TYPES], ["code"])
(
    df.filter(col("Actor1CountryCode").isNotNull())
    .join(types_df, df.Actor1CountryCode == types_df.code, "left_anti")
    .groupBy("Actor1CountryCode").count().orderBy(col("count").desc())
    .show(truncate=False)
)
PY
```

SSD : 1178 occurrences — un seul code distinct dans tout le bucket.

Interprétation : SSD est le code du Soudan du Sud, indépendant depuis
2011. CAMEO.country.txt ne contient pas cette entrée : référentiel
antérieur à l'indépendance, ou jamais mis à jour depuis. Ça explique
la totalité du bucket code_inconnu — pas une erreur de saisie côté
GDELT, un trou de couverture côté table de référence.

La table de référence officielle ne couvre donc pas entièrement le
flux : un seul code manquant, mais sur 2,7 M d'événements — pas un
problème de volume, un trou ponctuel et identifié. SSD ajouté dans
`src/cameo_country_types.py` comme "pays", avec un commentaire
précisant que c'est un ajout de notre part et non une entrée recopiée
de CAMEO.country.txt.

Décision de filtrage par défaut non prise ici : mesure seule, comme
demandé.

## Couverture géographique — axe 1, FIPS — 17 septembre 2026

`transform.py` mesure la jointure ActionGeo_CountryCode -> FIPS.country.txt :
2 595 904 lignes avec ActionGeo_CountryCode renseigné, 2 594 220 appariées
(99,935%), 1 684 non appariées (0,065%) sur 3 codes distincts : RB (1 457
occurrences), OC (163), YI (64). Recherche menée avant d'écrire quoi que ce
soit (Wikipedia FIPS 10-4, puis vérification directe sur les CSV bruts pour
ne pas se fier à une recherche seule) :

```bash
awk -F'\t' '$54=="OC" {print $53, "| type="$52}' data/raw/*.export.CSV | sort | uniq -c | sort -rn
awk -F'\t' '$54=="RB" {print $53, "| type="$52}' data/raw/*.export.CSV | sort | uniq -c | sort -rn
awk -F'\t' '$54=="YI" {print $53, "| type="$52}' data/raw/*.export.CSV | sort | uniq -c | sort -rn
```

(colonnes 52/53/54 = ActionGeo_Type / ActionGeo_Fullname / ActionGeo_CountryCode,
positions comptées dans `src/schema.py`.)

**OC** — 164 occurrences, ActionGeo_Fullname toujours "Indian Ocean",
"Pacific Ocean" ou "Atlantic Ocean", ActionGeo_Type = 4 (niveau
ville/point, pas pays). Pas une omission de la table FIPS : OC n'est pas
un code FIPS 10-4 du tout, c'est une extension du géocodeur de GDELT pour
placer un événement en haute mer, où aucun code pays n'a de sens. Une
recherche seule (sans vérifier les données) aurait fait conclure à tort
"OC = Océanie" (résultat obtenu via web search) — l'échantillon réel
contredit cette hypothèse et confirme l'importance de vérifier avant
d'écrire.

**RB** et **YI** — les deux désignent la Serbie, à deux époques du
référentiel FIPS 10-4 : YI = "Serbia and Montenegro" (code retiré en 2006
à la scission), RB = code de la Serbie seule après scission. Confirmé sur
l'échantillon : RB pointe vers "Belgrade, Serbia" (1 049 occurrences) et
des lignes de niveau pays sans nom (413) ; YI vers des lignes de niveau
pays sans nom (64, cohérent avec un code obsolète que le géocodeur émet
encore par endroits). Mais `data/reference/FIPS.country.txt` utilise ni
RB ni YI pour la Serbie : il contient `RI	Serbia` — une troisième
variante. Vérification : RI est bien utilisé par ailleurs dans les CSV
bruts (1 439 occurrences), donc GDELT émet les **trois** codes (RI, RB,
YI) pour le même pays selon les événements, sans cohérence interne, et
seul RI est couvert par notre table de référence.

Nature du trou, différente de SSD (Typologie des codes acteurs) : SSD
était une entrée manquante (pays jamais ajouté au référentiel). Ici,
RB/YI ne manquent pas au référentiel FIPS 10-4 en général (RB est le code
FIPS actuel de la Serbie) — c'est notre `FIPS.country.txt` qui n'a
qu'une entrée par pays (RI) quand GDELT en émet plusieurs pour le même
pays selon la version du géocodeur ou l'ancienneté de l'événement source.
Un vrai correctif demanderait une table de correspondance RB/YI -> RI,
pas un simple ajout de ligne comme pour SSD.

### Normalisation RB/YI -> RI, 18 septembre 2026

Décision : RB et YI normalisés vers RI, dans un petit dictionnaire dédié
`src/fips_country_aliases.py` (même logique que `cameo_country_types.py`
pour SSD : une correction de projet écrite en code, pas une modification
du fichier de référence téléchargé). OC reste volontairement en dehors
de ce dictionnaire et continue à ne pas matcher la table FIPS : ce n'est
pas un pays (voir ci-dessus, "haute mer"), l'exclure de la jointure de
l'axe 1 est correct, pas un trou à corriger.

`transform.py` applique la normalisation sur ActionGeo_CountryCode avant
la jointure FIPS de l'axe 1. Effet mesuré, avant/après, sur le Parquet
consolidé (`data/processed/events`) :

Événements Serbie (RI) avant normalisation : 1 420
Événements Serbie (RI) après normalisation (RB + YI regroupés) : 2 941
Récupérés par la normalisation : 1 521 (soit +107% par rapport aux 1 420
comptés sous RI seul — la majorité des événements Serbie du corpus
étaient donc perdus par la jointure interne de l'axe 1 avant ce
correctif, pas une poignée marginale)

Effet sur le taux de correspondance global de l'axe 1 : 99,935% avant
(1 684 non appariés sur RB/OC/YI) -> 99,994% après (163 non appariés,
uniquement OC, laissé tel quel par choix). Les 1 521 lignes récupérées
correspondent exactement à RB (1 457) + YI (64) mesurés précédemment :
aucune perte, aucun double comptage.

### Limite de cette détection — non exhaustive

RB et YI ont été trouvés uniquement parce qu'ils ne matchaient AUCUNE
entrée de `FIPS.country.txt` — ils sont apparus dans le bucket "non
apparié" de `measure_match_rate()`, qu'il a donc fallu examiner
manuellement (awk sur les CSV bruts) pour comprendre.

Cette méthode ne peut pas détecter le cas où un pays émettrait deux codes
FIPS distincts qui matchent chacun une entrée DIFFÉRENTE de
`FIPS.country.txt` — par exemple si `FIPS.country.txt` contenait par
erreur ou par mise à jour deux lignes pour un même pays sous deux codes
(cas hypothétique ici, mais structurellement possible pour n'importe quel
pays ayant changé de code FIPS au fil du temps, comme la Serbie).
Dans ce cas, aucune ligne ne serait non appariée : la jointure de l'axe 1
réussirait des deux côtés, silencieusement, et le pays serait compté deux
fois sous deux codes différents dans les agrégations. Rien dans ce
pipeline — ni `measure_match_rate()`, ni aucune autre mesure de
`transform.py` — ne peut détecter ce cas, puisqu'il ne produit aucune
ligne non appariée à examiner. Seule une revue manuelle de
`FIPS.country.txt` (chercher des noms de pays en double, pas des codes en
double) pourrait le révéler, non faite ici.

## Décomposition de l'exclusion du filtre entity_type — axe 2 — 17 septembre 2026

Le filtre `entity_type == "pays"` sur Actor1CountryCode ET
Actor2CountryCode (axe 2, `transform.py`) ne conserve que 572 425
événements sur 2 675 819 (21,393%), très en dessous des 53,08% de
"pays" mesurés sur Actor1 seul (section Typologie des codes acteurs) :
un filtre sur deux colonnes indépendantes ne peut être déduit d'un
filtre sur une seule. Décomposition par côté responsable de
l'exclusion :

```bash
python3 - << 'PY'
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from cameo_country_types import CAMEO_COUNTRY_TYPES

spark = SparkSession.builder.master("local[*]").appName("decompose-exclusions").getOrCreate()
events = spark.read.parquet("data/processed/events")
types_df = spark.createDataFrame(
    [(code, etype) for code, (_, etype) in CAMEO_COUNTRY_TYPES.items()],
    ["code", "entity_type"],
)
a1 = types_df.toDF("a1_code", "a1_type")
a2 = types_df.toDF("a2_code", "a2_type")
joined = (
    events
    .join(a1, events["Actor1CountryCode"] == a1["a1_code"], "left")
    .join(a2, events["Actor2CountryCode"] == a2["a2_code"], "left")
)
# ... filtres a1_type=="pays"/a2_type=="pays" combinés deux à deux, voir
# report pour le détail des quatre bucket (conservés, exclus_a1_seul,
# exclus_a2_seul, exclus_les_deux)
PY
```

conservés (Actor1 et Actor2 "pays") : 572 425, 21,393% du total
exclus par Actor1 seul (Actor1 non-pays, Actor2 "pays") : 516 148,
19,289% du total, 24,539% des exclusions
exclus par Actor2 seul (Actor2 non-pays, Actor1 "pays") : 848 970,
31,727% du total, 40,362% des exclusions
exclus par les deux (Actor1 et Actor2 non-pays) : 738 276, 27,591%
du total, 35,099% des exclusions
somme de contrôle : 572 425 + 516 148 + 848 970 + 738 276 = 2 675 819,
identique au total d'événements.

Réponse à la question posée : Actor2CountryCode vide n'écrase pas *tout*
à lui seul, mais c'est le facteur dominant. Décomposé par cause côté
Actor2 (region/territoire/null) sur les 1 587 246 lignes où Actor2 n'est
pas "pays" : null à 96,296% (1 528 447 occurrences), le reste
négligeable. Même décomposition côté Actor1 sur 1 254 424 lignes non-pays :
null à 95,014% (1 191 880). Actor2CountryCode est vide sur 57,121% de
tous les événements contre 44,543% pour Actor1CountryCode (cohérent avec
le codebook GDELT : un événement peut n'avoir qu'un seul acteur identifié,
Actor2 est structurellement moins souvent renseigné qu'Actor1). Ce
différentiel de 12,6 points explique l'essentiel de l'écart entre
exclus_actor1_seul (516 148) et exclus_actor2_seul (848 970). Mais le
bucket "exclus par les deux" (738 276, 27,591% du total) reste le
deuxième contributeur, pas négligeable : une part substantielle des
événements manque des deux côtés à la fois plutôt que d'un seul,
cohérence attendue si l'absence d'acteur identifié tient à la nature de
l'article source (dépêche factuelle sans acteurs nommés) plutôt qu'à un
défaut indépendant sur chaque colonne.
