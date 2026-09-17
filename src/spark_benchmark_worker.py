"""Sous-processus Spark isolé pour la comparaison 5 de benchmark.py.

À la différence des comparaisons 1-4 (SparkSession partagée, démarrée une
fois pour tout le fichier, JVM déjà chaude), ce script démarre sa PROPRE
SparkSession à froid pour chaque appel. La question posée par la
comparaison 5 est le seuil de volumétrie à partir duquel Spark devient
plus rapide que pandas ; ce seuil dépend directement du coût de démarrage
de la JVM, qui doit donc être payé à l'intérieur du chronomètre, pas
amorti comme dans le reste de ce fichier.

Symétrique à pandas_benchmark_worker.py : même liste de fichiers en
entrée, même agrégation (nombre de groupes distincts par
ActionGeo_CountryCode).

Appel : python spark_benchmark_worker.py <fichier_liste_chemins>
"""

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import SparkSession

from ingest import read_gdelt_events


def main() -> None:
    list_file = sys.argv[1]
    with open(list_file) as f:
        paths = [line.strip() for line in f if line.strip()]

    start = time.perf_counter()
    spark = SparkSession.builder.master("local[*]").appName("gdelt-benchmark-pandas-vs-spark-cold").getOrCreate()
    df = read_gdelt_events(spark, paths)
    n_groups = df.groupBy("ActionGeo_CountryCode").count().count()
    elapsed = time.perf_counter() - start
    spark.stop()

    print(json.dumps({"elapsed_s": elapsed, "n_groups": int(n_groups)}))


if __name__ == "__main__":
    main()
