"""Benchmarks exploratoires PySpark, avant décision sur clean.py et transform.py.

Cinq comparaisons indépendantes, chacune sur plusieurs exécutions pour
observer la variabilité (JVM déjà chaude, cache disque de l'OS déjà chaud)
plutôt qu'un chiffre unique qui la masquerait :

1. Effet de la consolidation : même agrégation (comptage d'événements par
   ActionGeo_CountryCode) sur les CSV bruts vs sur le Parquet consolidé.
2. Taille sur disque des deux formats (`du -sb`).
3. Dans clean.py, l'ordre actuel est cache -> count -> select. Comparé ici à
   select -> count sans cache, pour savoir si le cache aide ou coûte.
4. Dans transform.py, la jointure Actor1CountryCode -> table CAMEO est en
   broadcast hash join explicite. Comparé ici à un sort-merge join forcé
   (spark.sql.autoBroadcastJoinThreshold = -1), pour vérifier que le choix
   n'est pas seulement défendable en théorie mais mesurablement plus rapide.
5. pandas vs PySpark, même agrégation que la comparaison 1, sur les CSV
   bruts, à 1%/10%/100% du corpus. À la différence des comparaisons 1-4,
   exécutée dans des sous-processus isolés (voir pandas_benchmark_worker.py
   et spark_benchmark_worker.py) : mesure de pic mémoire propre à chaque
   run côté pandas, et JVM froide (pas la SparkSession partagée du reste
   de ce fichier) côté Spark, pour que le coût de démarrage fasse partie
   du chronomètre — condition nécessaire pour répondre à la question du
   seuil de volumétrie.

N'écrit rien dans report/mesures.md : affichage console seulement, la
décision de consigner ou non revient à l'auteur du pipeline.
"""

import argparse
import contextlib
import glob
import io
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col

from cameo_country_types import CAMEO_COUNTRY_TYPES
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


def compare_broadcast_strategy(spark: SparkSession, parquet_dir: str, n_runs: int) -> None:
    """Comparaison 4 : broadcast hash join explicite vs sort-merge forcé.

    Même jointure dans les deux cas (Actor1CountryCode -> table CAMEO, 262
    codes) ; seule la stratégie change. Le sort-merge est forcé via
    spark.sql.autoBroadcastJoinThreshold = -1 (désactive le broadcast
    automatique de Catalyst) plutôt que comparé à un hint absent : sans ce
    forçage, Catalyst broadcasterait de toute façon la petite table (262
    lignes très sous le seuil par défaut de 10 Mo), et on ne mesurerait
    rien de plus que la même stratégie exécutée deux fois.

    Le plan physique (.explain()) est capturé via redirect_stdout plutôt
    que via une API dédiée : PySpark 4.2 n'expose pas de version qui
    renvoie directement une chaîne, .explain() écrit toujours sur stdout.
    """
    print("\n=== Comparaison 4 : broadcast explicite vs sort-merge forcé ===")
    types_df = spark.createDataFrame(
        [(code, etype) for code, (_, etype) in CAMEO_COUNTRY_TYPES.items()],
        ["code", "entity_type"],
    )
    default_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

    strategies = [
        ("broadcast explicite", True, default_threshold),
        ("sort-merge forcé (autoBroadcastJoinThreshold=-1)", False, "-1"),
    ]

    for label, use_broadcast, threshold in strategies:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", threshold)
        print(f"\n--- {label} ---")
        for i in range(1, n_runs + 1):
            events = spark.read.parquet(parquet_dir)
            ref = broadcast(types_df) if use_broadcast else types_df
            joined = events.join(ref, events["Actor1CountryCode"] == ref["code"], "inner")

            if i == 1:
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    joined.explain(mode="extended")
                plan = buf.getvalue()
                join_kind = "BroadcastHashJoin" if "BroadcastHashJoin" in plan else (
                    "SortMergeJoin" if "SortMergeJoin" in plan else "autre"
                )
                has_shuffle = "Exchange hashpartitioning" in plan
                print(f"    plan (run 1) : {join_kind}, {'shuffle (Exchange hashpartitioning) présent' if has_shuffle else 'aucun shuffle'}")

            def action(joined=joined):
                return joined.count()

            n_matched, elapsed, n_tasks = run_action_and_measure(spark, action)
            print(f"run {i} : {elapsed:.2f}s, {n_tasks} tâches, {n_matched} lignes appariées")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", default_threshold)


def files_for_volumetry(raw_pattern: str, pct: float) -> list:
    """Sélectionne les N premiers fichiers (tri chronologique) pour une volumétrie donnée.

    Sous-ensemble de fichiers plutôt qu'échantillonnage aléatoire de
    lignes : reproductible, et mesure un vrai coût d'E/S à petite
    volumétrie plutôt que de supposer que tout a déjà été lu en mémoire.
    """
    all_files = sorted(glob.glob(raw_pattern))
    n = max(1, round(len(all_files) * pct))
    return [str(Path(p).resolve()) for p in all_files[:n]]


def run_worker(cmd: list, timeout: int) -> dict:
    """Exécute un sous-processus worker et normalise son résultat.

    Distingue trois échecs plutôt qu'un seul "ça a planté" : timeout
    (probable swap thrashing, aucun message d'erreur à donner), tué par
    signal (probable OOM killer du noyau, aucun message d'erreur Python
    récupérable), et exception Python normale (message d'erreur exact
    disponible dans stderr).
    """
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return {"status": "timeout"}

    if result.returncode < 0:
        return {"status": "killed", "signal": -result.returncode}
    if result.returncode != 0:
        return {"status": "error", "output": result.stdout + result.stderr}

    return {"status": "ok", **json.loads(result.stdout.strip().splitlines()[-1])}


def compare_pandas_vs_spark(raw_pattern: str, volumetries: list, timeout: int) -> None:
    """Comparaison 5 : pandas vs PySpark, même agrégation que la comparaison 1, à plusieurs volumétries.

    Voir les docstrings de pandas_benchmark_worker.py et
    spark_benchmark_worker.py pour le détail de l'isolation par
    sous-processus.
    """
    print("\n=== Comparaison 5 : pandas vs PySpark, plusieurs volumétries ===")
    src_dir = Path(__file__).resolve().parent
    pandas_worker = str(src_dir / "pandas_benchmark_worker.py")
    spark_worker = str(src_dir / "spark_benchmark_worker.py")

    with tempfile.TemporaryDirectory() as tmpdir:
        for pct in volumetries:
            files = files_for_volumetry(raw_pattern, pct)
            list_file = str(Path(tmpdir) / f"files_{pct}.txt")
            with open(list_file, "w") as f:
                f.write("\n".join(files))

            print(f"\n--- Volumétrie {100 * pct:g}% ({len(files)} fichiers) ---")

            spark_result = run_worker([sys.executable, spark_worker, list_file], timeout)
            if spark_result["status"] == "ok":
                print(f"Spark (froid) : {spark_result['elapsed_s']:.2f}s, {spark_result['n_groups']} groupes")
            else:
                print(f"Spark (froid) : ÉCHEC ({spark_result['status']})")
                if spark_result["status"] == "error":
                    print(spark_result["output"])

            pandas_result = run_worker([sys.executable, pandas_worker, list_file], timeout)
            if pandas_result["status"] == "ok":
                peak_mb = pandas_result["peak_rss_kb"] / 1024
                print(f"pandas        : {pandas_result['elapsed_s']:.2f}s, {pandas_result['n_groups']} groupes, pic mémoire {peak_mb:.1f} Mo")
            elif pandas_result["status"] == "timeout":
                print(f"pandas        : ÉCHEC (timeout {timeout}s dépassé — probable swap thrashing sous pression mémoire, pas d'exception Python à rapporter)")
            elif pandas_result["status"] == "killed":
                print(f"pandas        : ÉCHEC (tué par signal {pandas_result['signal']} — probable OOM killer du noyau, aucun message d'erreur Python récupérable)")
            else:
                print("pandas        : ÉCHEC")
                print(pandas_result["output"])

            if spark_result["status"] == "ok" and pandas_result["status"] == "ok":
                if spark_result["n_groups"] != pandas_result["n_groups"]:
                    print(f"ATTENTION : {spark_result['n_groups']} groupes côté Spark vs {pandas_result['n_groups']} côté pandas — pas de contrôle de cohérence possible")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-pattern", default="data/raw/*.export.CSV")
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--parquet-dir", default="data/processed/events")
    parser.add_argument("--start", type=int, default=20260801, help="SQLDATE minimum inclus (YYYYMMDD)")
    parser.add_argument("--end", type=int, default=20260830, help="SQLDATE maximum inclus (YYYYMMDD)")
    parser.add_argument("--n-runs", type=int, default=3)
    parser.add_argument(
        "--volumetries",
        default="0.01,0.10,1.0",
        help="Fractions du corpus pour la comparaison 5, ex. '0.01,0.10,1.0'",
    )
    parser.add_argument(
        "--pandas-timeout",
        type=int,
        default=600,
        help="Timeout en secondes pour chaque sous-processus de la comparaison 5",
    )
    parser.add_argument(
        "--comparisons",
        default="1,2,3,4,5",
        help="Sous-ensemble à exécuter, ex. '1,3'",
    )
    args = parser.parse_args()
    selected = set(args.comparisons.split(","))

    if selected - {"5"}:
        spark = SparkSession.builder.master("local[*]").appName("gdelt-benchmark").getOrCreate()

        if "1" in selected:
            compare_consolidation(spark, args.input_pattern, args.parquet_dir, args.n_runs)
        if "2" in selected:
            compare_disk_size(args.raw_dir, args.parquet_dir)
        if "3" in selected:
            compare_cache_strategy(spark, args.input_pattern, args.start, args.end, args.n_runs)
        if "4" in selected:
            compare_broadcast_strategy(spark, args.parquet_dir, args.n_runs)

        spark.stop()

    if "5" in selected:
        volumetries = [float(v) for v in args.volumetries.split(",")]
        compare_pandas_vs_spark(args.input_pattern, volumetries, args.pandas_timeout)


if __name__ == "__main__":
    main()
