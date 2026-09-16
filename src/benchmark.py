"""Benchmarks exploratoires PySpark, avant décision sur clean.py.

Trois comparaisons indépendantes, chacune sur plusieurs exécutions pour
observer la variabilité (JVM déjà chaude, cache disque de l'OS déjà chaud)
plutôt qu'un chiffre unique qui la masquerait :

1. Effet de la consolidation : même agrégation (comptage d'événements par
   ActionGeo_CountryCode) sur les CSV bruts vs sur le Parquet consolidé.
2. Taille sur disque des deux formats (`du -sb`).
3. Dans clean.py, l'ordre actuel est cache -> count -> select. Comparé ici à
   select -> count sans cache, pour savoir si le cache aide ou coûte.

N'écrit rien dans report/mesures.md : affichage console seulement, la
décision de consigner ou non revient à l'auteur du pipeline.
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from clean import KEPT_COLUMNS
from ingest import read_gdelt_events


def run_action_and_measure(spark: SparkSession, action_fn):
    """Exécute action_fn() et mesure temps écoulé + nombre de tâches Spark.

    Le comptage de tâches passe par statusTracker() plutôt que par un
    SparkListener : job IDs connus avant/après l'appel, puis somme de
    numTasks sur tous les stages des jobs apparus entre les deux. Nécessaire
    car une seule action peut déclencher plusieurs jobs Spark.
    """
    tracker = spark.sparkContext.statusTracker()
    before = set(tracker.getJobIdsForGroup(None) or [])

    start = time.perf_counter()
    result = action_fn()
    elapsed = time.perf_counter() - start

    after = set(tracker.getJobIdsForGroup(None) or [])
    total_tasks = 0
    for job_id in after - before:
        info = tracker.getJobInfo(job_id)
        if info is None:
            continue
        for stage_id in info.stageIds:
            stage_info = tracker.getStageInfo(stage_id)
            if stage_info is not None:
                total_tasks += stage_info.numTasks

    return result, elapsed, total_tasks


def compare_consolidation(spark: SparkSession, raw_pattern: str, parquet_dir: str, n_runs: int) -> None:
    """Comparaison 1 : comptage par ActionGeo_CountryCode, CSV bruts vs Parquet."""
    print("=== Comparaison 1 : effet de la consolidation ===")
    sources = [
        ("CSV bruts", lambda: read_gdelt_events(spark, raw_pattern)),
        ("Parquet consolidé", lambda: spark.read.parquet(parquet_dir)),
    ]
    for label, loader in sources:
        print(f"\n--- {label} ---")
        for i in range(1, n_runs + 1):
            df = loader()
            n_partitions = df.rdd.getNumPartitions()

            def action(df=df):
                return df.groupBy("ActionGeo_CountryCode").count().count()

            n_groups, elapsed, n_tasks = run_action_and_measure(spark, action)
            print(
                f"run {i} : {elapsed:.2f}s, {n_tasks} tâches, "
                f"{n_partitions} partitions, {n_groups} groupes"
            )


def compare_disk_size(raw_dir: str, parquet_dir: str) -> None:
    """Comparaison 2 : taille sur disque, mesurée par `du -sb` (même méthode que mesures.md)."""
    print("\n=== Comparaison 2 : taille sur disque ===")
    for label, path in [("CSV bruts", raw_dir), ("Parquet consolidé", parquet_dir)]:
        out = subprocess.run(["du", "-sb", path], capture_output=True, text=True, check=True)
        size = int(out.stdout.split()[0])
        print(f"{label} ({path}) : {size} octets")


def rdd_storage_info(spark: SparkSession) -> list:
    """Taille mémoire/disque réelle des RDD actuellement en cache.

    Pas d'API Python publique pour ça : on passe par le SparkContext Java
    sous-jacent (_jsc), qui expose getRDDStorageInfo() comme en Scala/Java.
    """
    return [
        {
            "mem_size": info.memSize(),
            "disk_size": info.diskSize(),
            "num_cached_partitions": info.numCachedPartitions(),
            "num_partitions": info.numPartitions(),
        }
        for info in spark.sparkContext._jsc.sc().getRDDStorageInfo()
    ]


def print_storage_info(spark: SparkSession, label: str) -> None:
    infos = rdd_storage_info(spark)
    if not infos:
        print(f"    [{label}] aucun RDD en cache trouvé")
        return
    for info in infos:
        spill = " -> SPILL DISQUE" if info["disk_size"] > 0 else " -> tout tient en mémoire"
        print(
            f"    [{label}] mem={info['mem_size']} octets, disk={info['disk_size']} octets{spill}, "
            f"{info['num_cached_partitions']}/{info['num_partitions']} partitions cachées"
        )


def compare_cache_strategy(spark: SparkSession, raw_pattern: str, start: int, end: int, n_runs: int) -> None:
    """Comparaison 3 : isole cache et projection au lieu de les confondre.

    Trois variantes plutôt que deux : "cache avant select" changeait deux
    choses à la fois par rapport à "select sans cache" (présence du cache
    ET largeur des données mises en cache, 61 colonnes vs 12). La variante
    "select puis cache" ne fait varier que le cache : mêmes 12 colonnes des
    deux côtés, avec ou sans cache. Si elle se rapproche de "sans cache",
    la projection était la vraie cause de l'écart précédent ; si elle se
    rapproche de "cache 61 colonnes", c'est bien un coût de cache (spill).
    """
    driver_memory = spark.sparkContext.getConf().get(
        "spark.driver.memory", "non fixé explicitement (défaut Spark 1g)"
    )
    print(f"\n=== Comparaison 3 : cache vs projection (spark.driver.memory = {driver_memory}) ===")

    def with_cache_61cols():
        raw = read_gdelt_events(spark, raw_pattern)
        raw.cache()
        total = raw.count()
        print_storage_info(spark, "cache avant select, 61 colonnes")
        windowed = raw.filter((col("SQLDATE") >= start) & (col("SQLDATE") <= end))
        kept = windowed.count()
        raw.unpersist()
        return total, kept

    def select_then_cache_12cols():
        raw = read_gdelt_events(spark, raw_pattern)
        selected = raw.select(*KEPT_COLUMNS)
        selected.cache()
        total = selected.count()
        print_storage_info(spark, "cache après select, 12 colonnes")
        windowed = selected.filter((col("SQLDATE") >= start) & (col("SQLDATE") <= end))
        kept = windowed.count()
        selected.unpersist()
        return total, kept

    def select_then_count_no_cache():
        raw = read_gdelt_events(spark, raw_pattern)
        selected = raw.select(*KEPT_COLUMNS)
        total = selected.count()
        windowed = selected.filter((col("SQLDATE") >= start) & (col("SQLDATE") <= end))
        kept = windowed.count()
        return total, kept

    strategies = [
        ("cache avant select, 61 colonnes (actuel)", with_cache_61cols),
        ("select puis cache, 12 colonnes", select_then_cache_12cols),
        ("select -> count, sans cache", select_then_count_no_cache),
    ]
    for label, pipeline in strategies:
        print(f"\n--- {label} ---")
        for i in range(1, n_runs + 1):
            (total, kept), elapsed, n_tasks = run_action_and_measure(spark, pipeline)
            print(f"run {i} : {elapsed:.2f}s, {n_tasks} tâches, total={total}, conservées={kept}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-pattern", default="data/raw/*.export.CSV")
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--parquet-dir", default="data/processed/events")
    parser.add_argument("--start", type=int, default=20260801, help="SQLDATE minimum inclus (YYYYMMDD)")
    parser.add_argument("--end", type=int, default=20260830, help="SQLDATE maximum inclus (YYYYMMDD)")
    parser.add_argument("--n-runs", type=int, default=3)
    parser.add_argument(
        "--comparisons",
        default="1,2,3",
        help="Sous-ensemble à exécuter, ex. '1,3'",
    )
    args = parser.parse_args()
    selected = set(args.comparisons.split(","))

    spark = SparkSession.builder.master("local[*]").appName("gdelt-benchmark").getOrCreate()

    if "1" in selected:
        compare_consolidation(spark, args.input_pattern, args.parquet_dir, args.n_runs)
    if "2" in selected:
        compare_disk_size(args.raw_dir, args.parquet_dir)
    if "3" in selected:
        compare_cache_strategy(spark, args.input_pattern, args.start, args.end, args.n_runs)

    spark.stop()


if __name__ == "__main__":
    main()
