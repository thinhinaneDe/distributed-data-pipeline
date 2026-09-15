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
