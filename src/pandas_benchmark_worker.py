"""Sous-processus pandas isolé pour la comparaison 5 de benchmark.py.

Isolé du processus benchmark.py principal pour deux raisons : (1)
resource.getrusage(RUSAGE_SELF).ru_maxrss est un pic cumulé depuis le
démarrage du PROCESSUS courant — mesurer plusieurs volumétries dans le
même processus pandas ferait fuir le pic d'un run dans le suivant ; (2) si
la volumétrie 100% épuise la mémoire, l'échec (MemoryError ou kill par
l'OOM killer du noyau) reste confiné à ce sous-processus, sans emporter le
processus benchmark.py ni sa SparkSession.

Lit les mêmes 61 colonnes typées que Spark (dtypes dérivés de
schema.GDELT_EVENTS_SCHEMA, pas dtype=str partout : un dtype=str générique
éviterait le coût de parsing numérique que Spark, lui, paie, et fausserait
la comparaison de temps).

Appel : python pandas_benchmark_worker.py <fichier_liste_chemins>
Sortie : une ligne JSON sur stdout ; tout le reste (erreurs, traceback) va
sur stderr, laissé tel quel pour que le processus parent le rapporte mot
pour mot en cas d'échec.
"""

import json
import resource
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd
from pyspark.sql.types import DoubleType, LongType

from schema import GDELT_EVENTS_SCHEMA


def pandas_dtypes_from_spark_schema() -> dict:
    """Traduit GDELT_EVENTS_SCHEMA en dtypes pandas, colonne par colonne.

    Dérivé du schéma Spark plutôt que réécrit à la main : les deux
    lectures doivent rester la même définition de colonnes, pas deux
    listes maintenues séparément.
    """
    dtypes = {}
    for field in GDELT_EVENTS_SCHEMA.fields:
        if isinstance(field.dataType, LongType):
            dtypes[field.name] = "Int64"  # nullable, GDELT a des colonnes entières avec valeurs manquantes
        elif isinstance(field.dataType, DoubleType):
            dtypes[field.name] = "float64"
        else:
            dtypes[field.name] = "object"
    return dtypes


def main() -> None:
    list_file = sys.argv[1]
    with open(list_file) as f:
        paths = [line.strip() for line in f if line.strip()]

    names = GDELT_EVENTS_SCHEMA.fieldNames()
    dtypes = pandas_dtypes_from_spark_schema()

    start = time.perf_counter()
    frames = [
        pd.read_csv(
            path,
            sep="\t",
            header=None,
            names=names,
            dtype=dtypes,
            quoting=3,  # csv.QUOTE_NONE, équivalent de .option("quote", "") côté Spark
            on_bad_lines="error",  # équivalent de mode FAILFAST côté Spark
        )
        for path in paths
    ]
    df = pd.concat(frames, ignore_index=True)
    # dropna=False : Spark compte le null comme un groupe à part entière
    # (groupBy().count() par défaut), pandas l'ignore par défaut. Même
    # champ vide -> null des deux côtés (nullValue="" par défaut côté
    # Spark), donc dropna=False est nécessaire pour rester comparable.
    n_groups = int(df.groupby("ActionGeo_CountryCode", dropna=False).size().shape[0])
    elapsed = time.perf_counter() - start

    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(json.dumps({"elapsed_s": elapsed, "n_groups": n_groups, "peak_rss_kb": peak_kb}))


if __name__ == "__main__":
    main()
