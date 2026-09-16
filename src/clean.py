"""Nettoyage des Events GDELT 2.0 et écriture du Parquet consolidé.

Une seule règle de nettoyage à ce stade : le filtre de fenêtre temporelle
(voir report/mesures.md, section Ingestion brute — quelques lignes
rediffusées portent un SQLDATE hors de la fenêtre de téléchargement).
Le nombre de lignes supprimées par ce filtre est compté et affiché, jamais
appliqué en silence.

Point de vigilance sur les colonnes de code pays conservées :
- Actor1CountryCode / Actor2CountryCode utilisent le référentiel CAMEO
  (codes à 3 lettres).
- ActionGeo_CountryCode utilise le référentiel FIPS 10-4 (codes à 2
  lettres).
Ce sont deux tables de correspondance différentes. Elles ne sont PAS
rapprochées ici. Le taux de correspondance de chaque jointure (axe 1 :
ActionGeo_CountryCode vs table FIPS ; axe 2 : Actor1/Actor2CountryCode vs
table CAMEO) sera mesuré et affiché dans les modules d'agrégation
correspondants, pas supposé.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from ingest import read_gdelt_events

# Colonnes retenues pour le Parquet consolidé, dérivées des deux axes
# d'agrégation prévus (couverture par pays d'action, interactions entre
# pays acteurs) plus les mesures génériques de poids d'un événement.
# Tout le reste (Ethnic/Religion/KnownGroup/Type2/Type3 : 97-100% vides sur
# la journée mesurée, et le détail géographique fin non utilisé par ces
# deux axes) est écarté.
KEPT_COLUMNS = [
    "GlobalEventID",
    "SQLDATE",
    "QuadClass",
    "GoldsteinScale",
    "AvgTone",
    "NumMentions",
    "NumSources",
    "NumArticles",
    "Actor1CountryCode",
    "Actor2CountryCode",
    "ActionGeo_CountryCode",
    "ActionGeo_Type",
]

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-pattern", default="data/raw/*.export.CSV")
    parser.add_argument("--start", type=int, default=20260801, help="SQLDATE minimum inclus (YYYYMMDD)")
    parser.add_argument("--end", type=int, default=20260830, help="SQLDATE maximum inclus (YYYYMMDD)")
    parser.add_argument("--output-dir", default="data/processed/events")
    args = parser.parse_args()

    spark = SparkSession.builder.master("local[*]").appName("gdelt-clean").getOrCreate()

    raw = read_gdelt_events(spark, args.input_pattern)
    # Projection avant tout comptage, sans cache (voir benchmark.py,
    # comparaison 3, et report/mesures.md section Benchmarks). Mettre en
    # cache les 61 colonnes brutes avant de compter deux fois semblait
    # justifié pour éviter un second passage disque, mais mesuré : ça
    # cachait ~440 Mo dont ~140 Mo spillés sur disque sous un driver à 1g
    # par défaut (spill plus coûteux qu'un simple re-read). Projeter les 12
    # colonnes utiles avant de compter fait tenir le nécessaire en mémoire
    # (~80 Mo, aucun spill mesuré) sans avoir besoin de cache ni de régler
    # spark.driver.memory.
    selected = raw.select(*KEPT_COLUMNS)
    total = selected.count()

    # SQLDATE fait partie de KEPT_COLUMNS : filtrer après la projection ne
    # perd aucune information nécessaire au filtre.
    windowed = selected.filter((col("SQLDATE") >= args.start) & (col("SQLDATE") <= args.end))
    kept = windowed.count()
    removed = total - kept

    print(f"Lignes lues                          : {total}")
    print(f"Filtre fenêtre temporelle [{args.start}, {args.end}] : {removed} lignes supprimées ({100 * removed / total:.3f}%)")
    print(f"Lignes conservées                    : {kept}")

    (
        windowed.write
        .mode("overwrite")
        .partitionBy("SQLDATE")
        .parquet(args.output_dir)
    )
    print(f"Parquet écrit dans {args.output_dir}/, partitionné par SQLDATE, {len(KEPT_COLUMNS)} colonnes.")

    spark.stop()

if __name__ == "__main__":
    main()
