"""StructType explicite pour les fichiers Events de GDELT 2.0 (gdeltv2/).

61 colonnes, tab-delimited, sans en-tête. Ordre et types basés sur
GDELT-Event_Codebook-V2.0.pdf (http://data.gdeltproject.org/documentation/
GDELT-Event_Codebook-V2.0.pdf), vérifiés colonne par colonne contre les
fichiers réellement téléchargés dans data/raw/ (voir report/mesures.md).

Pas d'inferSchema : sur 61 colonnes dont plusieurs peuvent être vides sur
une ligne donnée, la détection automatique de Spark peut deviner un type
différent d'un fichier à l'autre et casser un df.union() entre deux jours.
"""

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

GDELT_EVENTS_SCHEMA = StructType([
    # --- Identifiant et dates ---
    # GlobalEventID : le codebook le type "integer", et sa valeur mesurée le
    # 1er août 2026 tourne autour de 1,316 milliard (voir report/mesures.md).
    # C'est sous le plafond d'IntegerType (2 147 483 647), mais l'ID croît en
    # continu depuis 2015 sans jamais redescendre : LongType coûte 4 octets
    # de plus par ligne et élimine le risque de dépassement silencieux si le
    # pipeline est réexécuté dans quelques années.
    StructField("GlobalEventID", LongType(), nullable=False),

    # SQLDATE (nommé "Day" dans le codebook, "SQLDATE" dans les exports
    # réels et la doc BigQuery) : stocké en YYYYMMDD (ex. 20260801), donc un
    # entier et non un DateType Spark. Avantage : tri et filtre par plage
    # numérique sans parsing. Inconvénient : aucune arithmétique de date
    # native (pas de "+1 jour") sans conversion explicite en amont — à faire
    # dans l'étape de nettoyage, pas ici, pour garder ce fichier fidèle au
    # format brut.
    StructField("SQLDATE", LongType(), nullable=False),
    StructField("MonthYear", LongType(), nullable=False),   # YYYYMM
    StructField("Year", LongType(), nullable=False),        # YYYY

    # FractionDate : approximation volontaire du codebook (MONTH*30+DAY)/365,
    # ne tient compte ni des mois inégaux ni des années bissextiles. Un
    # DoubleType, mais à ne jamais utiliser comme vraie date — seulement
    # comme clé de tri approximative, tel que le codebook le prévoit.
    StructField("FractionDate", DoubleType(), nullable=False),

    # --- Actor1 ---
    StructField("Actor1Code", StringType(), nullable=True),
    StructField("Actor1Name", StringType(), nullable=True),
    StructField("Actor1CountryCode", StringType(), nullable=True),
    StructField("Actor1KnownGroupCode", StringType(), nullable=True),
    StructField("Actor1EthnicCode", StringType(), nullable=True),
    StructField("Actor1Religion1Code", StringType(), nullable=True),
    StructField("Actor1Religion2Code", StringType(), nullable=True),
    StructField("Actor1Type1Code", StringType(), nullable=True),
    StructField("Actor1Type2Code", StringType(), nullable=True),
    StructField("Actor1Type3Code", StringType(), nullable=True),

    # --- Actor2 (mêmes champs, même logique) ---
    StructField("Actor2Code", StringType(), nullable=True),
    StructField("Actor2Name", StringType(), nullable=True),
    StructField("Actor2CountryCode", StringType(), nullable=True),
    StructField("Actor2KnownGroupCode", StringType(), nullable=True),
    StructField("Actor2EthnicCode", StringType(), nullable=True),
    StructField("Actor2Religion1Code", StringType(), nullable=True),
    StructField("Actor2Religion2Code", StringType(), nullable=True),
    StructField("Actor2Type1Code", StringType(), nullable=True),
    StructField("Actor2Type2Code", StringType(), nullable=True),
    StructField("Actor2Type3Code", StringType(), nullable=True),

    # --- Action ---
    # IsRootEvent : flag 0/1, sémantiquement un booléen. Gardé en LongType
    # (pas BooleanType) pour rester fidèle au type "integer" du codebook et
    # à ce qui est réellement écrit dans le fichier (0/1, pas true/false) ;
    # la conversion en booléen, si utile, se fera en aval.
    StructField("IsRootEvent", LongType(), nullable=True),

    # EventCode / EventBaseCode / EventRootCode : le codebook déconseille
    # explicitement l'entier ici, à cause des codes CAMEO à zéro non
    # significatif ("010" vu en colonne 27 sur nos données réelles) — un
    # cast en Int ferait perdre ce zéro et confondrait des codes distincts.
    StructField("EventCode", StringType(), nullable=True),
    StructField("EventBaseCode", StringType(), nullable=True),
    StructField("EventRootCode", StringType(), nullable=True),

    # QuadClass : entier au sens strict (1 à 4, vérifié sur nos données),
    # mais c'est une catégorie nominale (1=Coopération verbale, 2=Coopération
    # matérielle, 3=Conflit verbal, 4=Conflit matériel), pas une magnitude —
    # gardé en LongType pour coller au codebook, mais ne jamais l'agréger
    # par moyenne/somme sans repasser par le libellé.
    StructField("QuadClass", LongType(), nullable=True),

    StructField("GoldsteinScale", DoubleType(), nullable=True),
    StructField("NumMentions", LongType(), nullable=True),
    StructField("NumSources", LongType(), nullable=True),
    StructField("NumArticles", LongType(), nullable=True),

    # AvgTone : le codebook le type juste "(numeric)", pas explicitement
    # "floating point" comme les autres — mais l'échelle (-100 à +100 avec
    # des valeurs courantes du type -3.5) impose DoubleType.
    StructField("AvgTone", DoubleType(), nullable=True),

    # --- Géographie Actor1 ---
    # Geo_Type : entier catégoriel (1=COUNTRY, 2=USSTATE, 3=USCITY,
    # 4=WORLDCITY, 5=WORLDSTATE), même remarque que QuadClass.
    StructField("Actor1Geo_Type", LongType(), nullable=True),
    StructField("Actor1Geo_Fullname", StringType(), nullable=True),
    StructField("Actor1Geo_CountryCode", StringType(), nullable=True),
    StructField("Actor1Geo_ADM1Code", StringType(), nullable=True),
    # ADM2Code : numérique (code GAUL) pour les lieux hors USA, mais
    # alphanumérique ("TX213") pour les lieux américains — un même champ,
    # deux formats selon le pays. StringType est la seule option cohérente.
    StructField("Actor1Geo_ADM2Code", StringType(), nullable=True),
    StructField("Actor1Geo_Lat", DoubleType(), nullable=True),
    StructField("Actor1Geo_Long", DoubleType(), nullable=True),
    # FeatureID : numérique signé pour un match ville (ex. -1044367, observé
    # en colonne 43 sur nos données), mais code texte (pays/ADM1) pour un
    # match pays ou état. Champ hétérogène par construction → StringType
    # obligatoire, jamais IntegerType même si la majorité des valeurs
    # ressemblent à des nombres.
    StructField("Actor1Geo_FeatureID", StringType(), nullable=True),

    # --- Géographie Actor2 (mêmes remarques) ---
    StructField("Actor2Geo_Type", LongType(), nullable=True),
    StructField("Actor2Geo_Fullname", StringType(), nullable=True),
    StructField("Actor2Geo_CountryCode", StringType(), nullable=True),
    StructField("Actor2Geo_ADM1Code", StringType(), nullable=True),
    StructField("Actor2Geo_ADM2Code", StringType(), nullable=True),
    StructField("Actor2Geo_Lat", DoubleType(), nullable=True),
    StructField("Actor2Geo_Long", DoubleType(), nullable=True),
    StructField("Actor2Geo_FeatureID", StringType(), nullable=True),

    # --- Géographie de l'action (mêmes remarques) ---
    StructField("ActionGeo_Type", LongType(), nullable=True),
    StructField("ActionGeo_Fullname", StringType(), nullable=True),
    StructField("ActionGeo_CountryCode", StringType(), nullable=True),
    StructField("ActionGeo_ADM1Code", StringType(), nullable=True),
    StructField("ActionGeo_ADM2Code", StringType(), nullable=True),
    StructField("ActionGeo_Lat", DoubleType(), nullable=True),
    StructField("ActionGeo_Long", DoubleType(), nullable=True),
    StructField("ActionGeo_FeatureID", StringType(), nullable=True),

    # --- Gestion des données ---
    # DATEADDED : YYYYMMDDHHMMSS (ex. 20260801000000, vérifié colonne 60 sur
    # nos données). Cette valeur dépasse largement le plafond d'IntegerType
    # (2 147 483 647) — ici LongType n'est pas une précaution, c'est une
    # nécessité : un IntegerType provoquerait soit une erreur de lecture,
    # soit une valeur tronquée silencieuse selon le mode Spark.
    StructField("DATEADDED", LongType(), nullable=False),
    StructField("SOURCEURL", StringType(), nullable=True),
])

assert len(GDELT_EVENTS_SCHEMA.fields) == 61
