"""Agrégations multi-niveaux et jointures avec tables de dimension.

Axe 1 — couverture par pays d'action : ActionGeo_CountryCode (référentiel
FIPS 10-4, 97,2% de complétude, voir report/mesures.md) rapproché de
data/reference/FIPS.country.txt. Agrégation par pays et par jour : nombre
d'événements, tonalité moyenne (AvgTone), répartition par QuadClass.

Axe 2 — interactions entre pays acteurs : Actor1CountryCode et
Actor2CountryCode (référentiel CAMEO) rapprochés de la classification de
cameo_country_types.py, chacun via sa propre jointure sur le même
DataFrame. Filtre entity_type == "pays" par défaut des deux côtés ; la
part exclue (region, territoire, code inconnu, ou null) est affichée, pas
appliquée en silence.

Les deux tables de référence (274 lignes pour FIPS, 262 pour CAMEO, sous
50 Ko chacune) sont négligeables face aux 2,7 M de lignes du Parquet
consolidé : jointure en broadcast hash join explicite des deux côtés
plutôt que de laisser Spark décider seul via
spark.sql.autoBroadcastJoinThreshold (10 Mo par défaut — les deux tables
seraient de toute façon broadcastées automatiquement, mais le choix est
écrit ici plutôt que délégué à un seuil de configuration invisible dans
ce fichier). Comparaison broadcast explicite vs sort-merge forcé mesurée
dans benchmark.py, comparaison 4, et consignée dans report/mesures.md.
Aucune des deux jointures ne peut souffrir de skew de jointure : broadcast
signifie qu'aucun côté n'est repartitionné par la clé, donc rien à
équilibrer sur cette étape (le skew possible sur la répartition des pays
eux-mêmes, ex. les États-Unis dominant le volume, relève de l'agrégation
qui suit, pas de la jointure).
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, broadcast, col, count, when

from cameo_country_types import CAMEO_COUNTRY_TYPES


def read_fips_countries(spark: SparkSession, path: str) -> DataFrame:
    """Lit FIPS.country.txt : 2 colonnes tab-delimited, sans en-tête, CRLF.

    Spark découpe les lignes sur \\r\\n comme sur \\n (LineRecordReader
    sous-jacent) : le \\r final n'atterrit pas dans la dernière colonne,
    pas de nettoyage supplémentaire nécessaire.
    """
    return (
        spark.read
        .option("sep", "\t")
        .option("header", "false")
        .csv(path)
        .toDF("fips_code", "country_name")
    )


def build_cameo_types_df(spark: SparkSession) -> DataFrame:
    """Construit un petit DataFrame (code, entity_type) depuis CAMEO_COUNTRY_TYPES.

    Un DataFrame plutôt qu'un dict Python capturé dans une UDF : ça permet
    un vrai plan de jointure (visible dans .explain(), broadcastable
    explicitement), cohérent avec l'axe 1 sur FIPS, plutôt qu'un mélange
    de deux façons de rapprocher un code d'un référentiel dans le même
    fichier.
    """
    return spark.createDataFrame(
        [(code, etype) for code, (_, etype) in CAMEO_COUNTRY_TYPES.items()],
        ["code", "entity_type"],
    )


def measure_match_rate(df: DataFrame, code_col: str, ref: DataFrame, ref_code_col: str, label: str) -> None:
    """Mesure et affiche le taux de correspondance d'une jointure code -> référentiel.

    Ne compte que les lignes où code_col est renseigné : un code absent
    (null) est un défaut de complétude de la source, déjà mesuré ailleurs
    (ActionGeo_CountryCode à 97,2%), pas un défaut de la table de
    référence. Les codes non appariés sont listés par left_anti (borné par
    le nombre de codes distincts, jamais par le volume d'événements :
    collect() sans risque ici).
    """
    non_null = df.filter(col(code_col).isNotNull())
    total = non_null.count()
    matched = (
        non_null.join(broadcast(ref), non_null[code_col] == ref[ref_code_col], "left_semi")
        .count()
    )
    unmatched = total - matched

    print(f"--- Correspondance {label} ---")
    print(f"Lignes avec {code_col} renseigné : {total}")
    print(f"Appariées à la table de référence : {matched} ({100 * matched / total:.3f}%)")
    print(f"Non appariées : {unmatched} ({100 * unmatched / total:.3f}%)")
    if unmatched > 0:
        rows = (
            non_null.join(broadcast(ref), non_null[code_col] == ref[ref_code_col], "left_anti")
            .groupBy(code_col).count().orderBy(col("count").desc())
            .collect()
        )
        for row in rows:
            print(f"  code non apparié {row[code_col]} : {row['count']} occurrences")


def build_action_geo_aggregates(events: DataFrame, fips: DataFrame) -> DataFrame:
    """Axe 1 : agrégation par ActionGeo_CountryCode et par jour."""
    joined = events.filter(col("ActionGeo_CountryCode").isNotNull()).join(
        broadcast(fips), events["ActionGeo_CountryCode"] == fips["fips_code"], "inner"
    )
    return (
        joined
        .groupBy("ActionGeo_CountryCode", "country_name", "SQLDATE")
        .agg(
            count("*").alias("n_events"),
            avg("AvgTone").alias("avg_tone"),
            count(when(col("QuadClass") == 1, True)).alias("n_verbal_cooperation"),
            count(when(col("QuadClass") == 2, True)).alias("n_material_cooperation"),
            count(when(col("QuadClass") == 3, True)).alias("n_verbal_conflict"),
            count(when(col("QuadClass") == 4, True)).alias("n_material_conflict"),
        )
        .orderBy("SQLDATE", "ActionGeo_CountryCode")
    )


def build_actor_interactions(events: DataFrame, types_df: DataFrame, entity_type: str) -> DataFrame:
    """Axe 2 : interactions Actor1CountryCode / Actor2CountryCode, filtrées par entity_type.

    Deux jointures broadcast sur le même DataFrame (une par acteur), pas
    une seule jointure sur une clé composée : Actor1 et Actor2 partagent
    le même référentiel CAMEO mais sont deux colonnes indépendantes.
    """
    a1_types = types_df.toDF("a1_code", "a1_type")
    a2_types = types_df.toDF("a2_code", "a2_type")

    total = events.count()
    joined = (
        events
        .join(broadcast(a1_types), events["Actor1CountryCode"] == a1_types["a1_code"], "left")
        .join(broadcast(a2_types), events["Actor2CountryCode"] == a2_types["a2_code"], "left")
    )
    kept = joined.filter((col("a1_type") == entity_type) & (col("a2_type") == entity_type))
    n_kept = kept.count()
    excluded = total - n_kept

    print(f"--- Interactions Actor1/Actor2, filtre entity_type == '{entity_type}' ---")
    print(f"Événements totaux : {total}")
    print(f"Conservés (les deux côtés '{entity_type}') : {n_kept} ({100 * n_kept / total:.3f}%)")
    print(f"Exclus (null, region, territoire ou code inconnu d'un côté au moins) : {excluded} ({100 * excluded / total:.3f}%)")

    return (
        kept
        .groupBy("Actor1CountryCode", "Actor2CountryCode")
        .agg(count("*").alias("n_events"), avg("AvgTone").alias("avg_tone"))
        .orderBy(col("n_events").desc())
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", default="data/processed/events")
    parser.add_argument("--reference-dir", default="data/reference")
    parser.add_argument("--output-dir", default="data/processed/aggregates")
    parser.add_argument("--entity-type", default="pays", help="Filtre entity_type pour l'axe 2 (voir cameo_country_types.py)")
    args = parser.parse_args()

    spark = SparkSession.builder.master("local[*]").appName("gdelt-transform").getOrCreate()

    events = spark.read.parquet(args.input_dir)
    fips = read_fips_countries(spark, f"{args.reference_dir}/FIPS.country.txt")
    cameo_types = build_cameo_types_df(spark)

    print("=== Axe 1 : couverture par pays d'action (FIPS) ===")
    measure_match_rate(events, "ActionGeo_CountryCode", fips, "fips_code", "ActionGeo_CountryCode -> FIPS")
    action_geo = build_action_geo_aggregates(events, fips)
    (
        action_geo.write
        .mode("overwrite")
        .partitionBy("SQLDATE")
        .parquet(f"{args.output_dir}/action_geo_daily")
    )
    print(f"Parquet écrit dans {args.output_dir}/action_geo_daily/, partitionné par SQLDATE.")

    print("\n=== Axe 2 : interactions entre pays acteurs (CAMEO) ===")
    measure_match_rate(events, "Actor1CountryCode", cameo_types, "code", "Actor1CountryCode -> CAMEO")
    measure_match_rate(events, "Actor2CountryCode", cameo_types, "code", "Actor2CountryCode -> CAMEO")
    actor_interactions = build_actor_interactions(events, cameo_types, args.entity_type)
    (
        actor_interactions.write
        .mode("overwrite")
        .parquet(f"{args.output_dir}/actor_interactions")
    )
    print(f"Parquet écrit dans {args.output_dir}/actor_interactions/.")

    spark.stop()


if __name__ == "__main__":
    main()
