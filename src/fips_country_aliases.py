"""Normalisation de codes FIPS alternatifs émis par GDELT vers le code
canonique présent dans data/reference/FIPS.country.txt.

Voir report/mesures.md, section "Couverture géographique — axe 1, FIPS" :
ce fichier ne contient qu'une entrée par pays (RI pour la Serbie), mais le
géocodeur de GDELT émet plusieurs codes FIPS 10-4 différents pour ce même
pays selon la version du référentiel ou l'ancienneté de l'événement
source repris par l'article :

- RB : code FIPS 10-4 actuel de la Serbie (après la scission Serbie-
  Monténégro de 2006). Absent de FIPS.country.txt, qui utilise RI à la
  place pour le même pays.
- YI : code FIPS 10-4 retiré en 2006, "Serbia and Montenegro" (union
  d'avant scission). Toujours émis par endroits par le géocodeur GDELT.

OC (codes des océans, voir même section du rapport) n'est volontairement
PAS dans ce dictionnaire : ce n'est pas un pays, le laisser non apparié
par la jointure FIPS est le comportement correct, pas un trou à corriger.

Portée limitée de cette détection, documentée dans report/mesures.md :
RB et YI ont été trouvés parce qu'ils ne matchaient AUCUNE entrée de
FIPS.country.txt (bucket "non apparié" de la jointure). Un pays émettant
deux codes FIPS distincts qui matchent chacun une entrée DIFFÉRENTE de
FIPS.country.txt ne produirait aucune ligne non appariée, et ne serait
donc pas détecté par cette méthode : il resterait silencieusement compté
comme deux pays distincts dans les agrégations de l'axe 1, sans qu'aucune
mesure de ce pipeline ne le signale.
"""

FIPS_COUNTRY_ALIASES: dict[str, str] = {
    "RB": "RI",  # Serbie (code FIPS 10-4 actuel) -> code utilisé par FIPS.country.txt
    "YI": "RI",  # Serbie-et-Monténégro (code retiré en 2006) -> code utilisé par FIPS.country.txt
}
