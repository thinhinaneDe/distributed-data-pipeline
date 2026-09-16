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
