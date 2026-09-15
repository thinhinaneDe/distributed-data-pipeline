# Pipeline de données distribuées (PySpark)

## 1. Contexte et question traitée

_À rédiger._

## 2. Données

- **Source :**
- **Volumétrie :**
- **Schéma :**
- **Comment les obtenir :**

## 3. Méthode

_Étapes du pipeline, dans l'ordre._

## 4. Résultats chiffrés

_Tableau du benchmark (pandas vs PySpark)._

## 5. Analyse

_Pourquoi ces écarts de performance._

## 6. Limites et pistes d'amélioration

## 7. Reproduction

```bash
make venv       # crée le venv et installe requirements.txt
make ingest     # télécharge / lit les données brutes
make clean      # nettoyage
make aggregate  # agrégations et jointure
make bench      # benchmark pandas vs Spark
```

`tests/smoke_test_spark.py` est un test de plomberie (pas le pipeline) : il
vérifie que la chaîne Java/PySpark/Parquet fonctionne. À lancer avec
`python tests/smoke_test_spark.py` après `make venv`.
