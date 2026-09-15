"""Script jetable : vérifie que GDELT_EVENTS_SCHEMA lit un fichier réel sans erreur.

Mode FAILFAST : Spark lève une exception au premier écart entre les données
et le schéma déclaré (mauvais nombre de colonnes, valeur non castable dans
le type déclaré), au lieu du mode PERMISSIVE par défaut qui remplirait ces
lignes de null en silence. Sert uniquement à valider schema.py, ne fait pas
partie du pipeline.
"""

import glob
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, when

from schema import GDELT_EVENTS_SCHEMA

FILE_PATTERN = sys.argv[1] if len(sys.argv) > 1 else "data/raw/20260801000000.export.CSV"

def main() -> None:
    spark = SparkSession.builder.master("local[*]").appName("check-schema").getOrCreate()

    df = (
        spark.read
        # les fichiers GDELT ont une extension .csv mais ne sont pas du CSV
        # RFC 4180 échappé par des guillemets : on désactive l'interprétation
        # du caractère guillemet pour ne pas fusionner des lignes par erreur
        # si un champ texte (Actor1Name, SOURCEURL) contient un `"` isolé.
        .option("quote", "")
        .option("sep", "\t")
        .option("header", "false")
        .option("mode", "FAILFAST")
        .schema(GDELT_EVENTS_SCHEMA)
        .csv(FILE_PATTERN)
    )

    spark_count = df.count()
    print(f"Périmètre : {FILE_PATTERN}")
    print(f"Lignes lues par Spark (FAILFAST, sans erreur) : {spark_count}")

    matched_files = sorted(glob.glob(FILE_PATTERN))
    wc_result = subprocess.run(["wc", "-l", *matched_files], capture_output=True, text=True, check=True)
    wc_count = int(wc_result.stdout.strip().split("\n")[-1].split()[0])
    print(f"Lignes comptées par wc -l ({len(matched_files)} fichier(s))    : {wc_count}")

    if spark_count != wc_count:
        print(f"\nÉCART détecté : Spark={spark_count} vs wc -l={wc_count}. Ne pas corriger, à signaler tel quel.")
    else:
        print("\nComptes identiques.")

    print("\nTaux de null par colonne (triées du plus au moins vide) :")
    null_counts = df.select([
        spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()

    for c, n in sorted(null_counts.items(), key=lambda kv: -kv[1]):
        pct = 100 * n / spark_count if spark_count else 0
        print(f"  {c:30s} {n:8d}  ({pct:5.1f}%)")

    spark.stop()

if __name__ == "__main__":
    main()
