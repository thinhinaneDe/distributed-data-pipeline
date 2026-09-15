"""Téléchargement des exports GDELT 2.0 Events bruts (gdeltv2, tranches de 15 min).

Récupère les fichiers .export.CSV.zip (pas .mentions, pas .gkg) sur une
plage de dates, les décompresse, et les range dans un dossier de sortie,
un fichier par tranche de 15 minutes. Ne fait que ça : pas de lecture
Spark/pandas, pas de schéma. Idempotent — un fichier déjà présent est
sauté.
"""

import argparse
import sys
import time
import urllib.error
import urllib.request
import zipfile
from datetime import date, timedelta
from pathlib import Path

BASE_URL = "https://data.gdeltproject.org/gdeltv2/{ts}.export.CSV.zip"

def daterange(start: date, end: date):
    for n in range((end - start).days + 1):
        yield start + timedelta(days=n)

def quarter_hour_timestamps(day: date):
    day_str = day.strftime("%Y%m%d")
    for hour in range(24):
        for minute in (0, 15, 30, 45):
            yield f"{day_str}{hour:02d}{minute:02d}00"

def download_slice(ts: str, out_dir: Path, pause: float) -> int:
    """Télécharge et décompresse une tranche de 15 min. Retourne la taille décompressée (octets), 0 si absente/échec."""
    csv_path = out_dir / f"{ts}.export.CSV"
    if csv_path.exists():
        return csv_path.stat().st_size

    url = BASE_URL.format(ts=ts)
    zip_path = out_dir / f"{ts}.export.CSV.zip"
    try:
        urllib.request.urlretrieve(url, zip_path)
    except urllib.error.HTTPError as exc:
        print(f"  {ts} : absent ({exc.code}), ignoré", file=sys.stderr)
        return 0

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(out_dir)
    zip_path.unlink()

    time.sleep(pause)
    return csv_path.stat().st_size if csv_path.exists() else 0

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="date de début, format YYYYMMDD")
    parser.add_argument("--end", required=True, help="date de fin (incluse), format YYYYMMDD")
    parser.add_argument("--out-dir", default="data/raw", help="dossier de sortie (défaut: data/raw)")
    parser.add_argument("--pause", type=float, default=0.2, help="pause en secondes entre téléchargements (défaut: 0.2)")
    args = parser.parse_args()

    start = date.fromisoformat(f"{args.start[:4]}-{args.start[4:6]}-{args.start[6:]}")
    end = date.fromisoformat(f"{args.end[:4]}-{args.end[4:6]}-{args.end[6:]}")
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    total_bytes = 0
    n_slices = 0
    for day in daterange(start, end):
        for ts in quarter_hour_timestamps(day):
            size = download_slice(ts, out_dir, args.pause)
            if size:
                total_bytes += size
                n_slices += 1
        print(f"  {day.isoformat()} : {n_slices} tranches cumulées, {total_bytes / 1_000_000:.1f} Mo décompressés")

    print(f"\n{n_slices} tranches de 15 min récupérées, {total_bytes / 1_000_000_000:.3f} Go décompressés au total dans {out_dir}/")

if __name__ == "__main__":
    main()
