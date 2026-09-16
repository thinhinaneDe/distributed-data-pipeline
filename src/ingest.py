"""Ingestion des fichiers Events GDELT 2.0 bruts, avec schéma explicite.

Ne fait que lire et valider la structure (FAILFAST) : pas de filtre, pas de
sélection de colonnes, pas d'écriture. Voir clean.py pour la suite.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import DataFrame, SparkSession

from schema import GDELT_EVENTS_SCHEMA

def read_gdelt_events(spark: SparkSession, path_pattern: str) -> DataFrame:
    """Lit les fichiers Events GDELT 2.0 correspondant à path_pattern.

    FAILFAST : toute ligne dont le nombre de colonnes ou un type de valeur
    ne correspond pas à GDELT_EVENTS_SCHEMA fait échouer la lecture, plutôt
    que d'être silencieusement remplie de null (comportement PERMISSIVE par
    défaut de Spark).
    """
    return (
        spark.read
        .option("quote", "")  # fichiers tab-delimited, pas du CSV échappé
        .option("sep", "\t")
        .option("header", "false")
        .option("mode", "FAILFAST")
        .schema(GDELT_EVENTS_SCHEMA)
        .csv(path_pattern)
    )

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-pattern", default="data/raw/*.export.CSV")
    args = parser.parse_args()

    spark = SparkSession.builder.master("local[*]").appName("gdelt-ingest").getOrCreate()
    df = read_gdelt_events(spark, args.input_pattern)
    print(f"Lignes lues ({args.input_pattern}) : {df.count()}")
    spark.stop()

if __name__ == "__main__":
    main()
