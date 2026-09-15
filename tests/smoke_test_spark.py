"""Test de plomberie — pas le pipeline.

Valide que la chaîne Java / PySpark / Parquet fonctionne :
démarrage de la JVM, écriture Parquet, relecture. À supprimer une
fois la validation faite.
"""

import shutil
import tempfile

from pyspark.sql import SparkSession

def main() -> None:
    spark = SparkSession.builder.master("local[*]").appName("smoke-test").getOrCreate()

    data = [(1, "a"), (2, "b"), (3, "c")]
    df = spark.createDataFrame(data, schema=["id", "label"])

    out_dir = tempfile.mkdtemp(prefix="spark_smoke_test_")
    try:
        df.write.mode("overwrite").parquet(out_dir)
        reread = spark.read.parquet(out_dir)

        assert reread.count() == df.count(), "nombre de lignes différent après relecture"
        reread.show()
        print("OK : écriture/lecture Parquet fonctionnelle.")
    finally:
        shutil.rmtree(out_dir, ignore_errors=True)
        spark.stop()

if __name__ == "__main__":
    main()
