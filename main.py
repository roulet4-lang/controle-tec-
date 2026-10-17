import asyncio
import csv
import os
import sqlite3
import tempfile
import time
import unicodedata
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict

# ============================================================
# CONFIGURATION
# ============================================================

APP_VERSION = "1.4.0-sqlite"
STATIC_URL_PATH = "/api/gtfs/feed/tec/static"
STATIC_CACHE_TTL_SECONDS = max(300, int(os.getenv("STATIC_CACHE_TTL_SECONDS", "21600")))
TMP_DIR = Path(os.getenv("GTFS_TMP_DIR", tempfile.gettempdir()))
ZIP_PATH = TMP_DIR / "tec_gtfs_static.zip"
DB_PATH = TMP_DIR / "tec_gtfs_static.sqlite3"
BUILD_DB_PATH = TMP_DIR / "tec_gtfs_static.building.sqlite3"
BRUSSELS = ZoneInfo("Europe/Brussels")

app = FastAPI(
    title="TEC Service Contrôle API",
    description="Passerelle vers les flux officiels TEC et le référentiel GTFS statique.",
    version=APP_VERSION,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://roulet4-lang.github.io"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

API_KEY = os.getenv("BM_API_KEY", "").strip()
API_BASE = os.getenv("BM_API_BASE", "").strip().rstrip("/")
FEEDS = {
    "vehicles": "/api/gtfs/feed/tec/rt/vehicle-position",
    "trips": "/api/gtfs/feed/tec/rt/trip-update",
    "alerts": "/api/gtfs/feed/tec/rt/alert",
}
_static_lock = asyncio.Lock()
_static_loaded_at = 0.0
_static_loaded_iso = None
_static_counts = None

SOURCE_STATIC = "GTFS statique officiel TEC via Belgian Mobility"


def normalize_name(value: str) -> str:
    value = unicodedata.normalize("NFKD", value or "")
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return " ".join("".join(ch.lower() if ch.isalnum() else " " for ch in value).split())


def time_seconds(value: str) -> int:
    try:
        h, m, s = (int(part) for part in value.split(":"))
        return h * 3600 + m * 60 + s
    except (ValueError, TypeError):
        return 0


def display_gtfs_time(value: str) -> str:
    total = time_seconds(value)
    days, remainder = divmod(total, 86400)
    hh, rem = divmod(remainder, 3600)
    mm = rem // 60
    return f"{hh:02d}:{mm:02d}" + (f" (+{days} j)" if days else "")


def db_connect(path: Path = DB_PATH) -> sqlite3.Connection:
    con = sqlite3.connect(str(path), timeout=30)
    con.row_factory = sqlite3.Row
    return con


def table_exists() -> bool:
    return DB_PATH.exists() and DB_PATH.stat().st_size > 0


# ============================================================
# TEMPS RÉEL — API GTFS-RT EXISTANTE
# ============================================================

async def fetch_feed(path: str) -> dict:
    if not API_KEY or not API_BASE:
        raise HTTPException(status_code=503, detail="Configuration Render incomplète : BM_API_KEY ou BM_API_BASE manquante.")
    if not API_BASE.startswith("https://"):
        raise HTTPException(status_code=503, detail="BM_API_BASE doit commencer par https://")
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                API_BASE + path,
                params={"format": "protobuf"},
                headers={"Ocp-Apim-Subscription-Key": API_KEY, "Accept": "application/x-protobuf"},
            )
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Délai dépassé lors de la connexion à Belgian Mobility.")
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Erreur réseau vers Belgian Mobility : {type(exc).__name__}.")

    if response.status_code in (401, 403):
        raise HTTPException(status_code=502, detail=f"Accès refusé par Belgian Mobility (HTTP {response.status_code}). Vérifie l'abonnement, la clé et les autorisations.")
    if response.status_code == 404:
        raise HTTPException(status_code=502, detail=f"Endpoint introuvable (HTTP 404) : {path}. Vérifie l'URL de base, le chemin et les paramètres dans le portail Belgian Mobility.")
    if response.status_code == 429:
        raise HTTPException(status_code=503, detail="Limite de requêtes Belgian Mobility atteinte.")
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Belgian Mobility a répondu HTTP {response.status_code} pour {path}.")

    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(response.content)
    except Exception:
        raise HTTPException(status_code=502, detail=f"Réponse reçue pour {path}, mais impossible de la décoder en GTFS-Realtime protobuf.")
    data = MessageToDict(feed, preserving_proto_field_name=True, use_integers_for_enums=True)
    header = data.get("header", {})
    entities = data.get("entity", [])
    return {
        "source": "Belgian Mobility / TEC",
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "feed_timestamp": header.get("timestamp"),
        "feed_version": header.get("gtfs_realtime_version"),
        "incrementality": header.get("incrementality"),
        "count": len(entities),
        "entities": entities,
    }


@app.get("/")
def home():
    return {
        "service": "TEC Service Contrôle API",
        "version": APP_VERSION,
        "status": "online",
        "health": "/health",
        "docs": "/docs",
        "connection_test": "/api/tec/connection-test",
        "static_summary": "/api/tec/static/summary",
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "version": APP_VERSION,
        "api_key_configured": bool(API_KEY),
        "api_base_configured": bool(API_BASE),
        "static_database_exists": table_exists(),
        "static_database_bytes": DB_PATH.stat().st_size if table_exists() else 0,
    }


@app.get("/api/tec/connection-test")
async def connection_test():
    results = {}
    for name, path in FEEDS.items():
        try:
            data = await fetch_feed(path)
            results[name] = {"ok": True, "count": data["count"], "feed_timestamp": data["feed_timestamp"]}
        except HTTPException as exc:
            results[name] = {"ok": False, "error": exc.detail}
    return {"all_ok": all(item["ok"] for item in results.values()), "checked_at": datetime.now(timezone.utc).isoformat(), "feeds": results}


@app.get("/api/tec/vehicles")
async def vehicles():
    return await fetch_feed(FEEDS["vehicles"])


@app.get("/api/tec/trips")
async def trips():
    return await fetch_feed(FEEDS["trips"])


@app.get("/api/tec/alerts")
async def alerts():
    return await fetch_feed(FEEDS["alerts"])


# ============================================================
# GTFS STATIQUE — TÉLÉCHARGEMENT EN FLUX + SQLITE (RAM LIMITÉE)
# ============================================================

def _download_and_build_db(zip_path: Path, build_path: Path) -> dict:
    """Parse le ZIP en streaming dans SQLite : aucune table GTFS complète en RAM."""
    if build_path.exists():
        build_path.unlink()
    if not zip_path.exists():
        raise RuntimeError("Archive GTFS temporaire introuvable")

    con = sqlite3.connect(str(build_path), timeout=60)
    try:
        con.execute("PRAGMA journal_mode=OFF")
        con.execute("PRAGMA synchronous=OFF")
        con.execute("PRAGMA temp_store=FILE")
        con.execute("PRAGMA cache_size=-8000")  # environ 8 Mo de cache SQLite
        con.executescript("""
            CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE routes (
                route_id TEXT PRIMARY KEY, short_name TEXT NOT NULL, long_name TEXT NOT NULL, route_type TEXT NOT NULL
            );
            CREATE TABLE trips (
                trip_id TEXT PRIMARY KEY, route_id TEXT NOT NULL, service_id TEXT NOT NULL,
                headsign TEXT NOT NULL, direction_id TEXT NOT NULL
            );
            CREATE TABLE stops (
                stop_id TEXT PRIMARY KEY, name TEXT NOT NULL, name_norm TEXT NOT NULL,
                lat REAL, lon REAL, parent_station TEXT
            );
            CREATE TABLE calendars (
                service_id TEXT PRIMARY KEY, start_date TEXT NOT NULL, end_date TEXT NOT NULL,
                monday INTEGER, tuesday INTEGER, wednesday INTEGER, thursday INTEGER,
                friday INTEGER, saturday INTEGER, sunday INTEGER
            );
            CREATE TABLE calendar_dates (
                service_id TEXT NOT NULL, service_date TEXT NOT NULL, exception_type INTEGER NOT NULL,
                PRIMARY KEY(service_id, service_date)
            );
            CREATE TABLE stop_times (
                stop_id TEXT NOT NULL, trip_id TEXT NOT NULL, service_id TEXT NOT NULL,
                route_id TEXT NOT NULL, arrival_time TEXT NOT NULL, departure_time TEXT NOT NULL,
                departure_seconds INTEGER NOT NULL, stop_sequence INTEGER NOT NULL
            );
            CREATE TABLE raw_stop_times (
                stop_id TEXT NOT NULL, trip_id TEXT NOT NULL, arrival_time TEXT NOT NULL,
                departure_time TEXT NOT NULL, stop_sequence INTEGER NOT NULL
            );
        """)

        with zipfile.ZipFile(zip_path, "r") as archive:
            filemap = {}
            for member in archive.namelist():
                base = member.rsplit("/", 1)[-1]
                if base.endswith(".txt") and base not in filemap:
                    filemap[base] = member
            required = {"stops.txt", "routes.txt", "trips.txt", "stop_times.txt"}
            if not required.issubset(filemap):
                raise ValueError("fichiers GTFS obligatoires absents : " + ", ".join(sorted(required - set(filemap))))

            def rows(filename):
                with archive.open(filemap[filename], "r") as raw:
                    import io
                    with io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as text_stream:
                        yield from csv.DictReader(text_stream)

            # Tables de référence : insertion par lots.
            batch = []
            for row in rows("routes.txt"):
                rid = (row.get("route_id") or "").strip()
                if rid:
                    batch.append((rid, (row.get("route_short_name") or "").strip(), (row.get("route_long_name") or "").strip(), (row.get("route_type") or "").strip()))
                if len(batch) >= 2000:
                    con.executemany("INSERT OR REPLACE INTO routes VALUES (?,?,?,?)", batch); batch.clear()
            if batch: con.executemany("INSERT OR REPLACE INTO routes VALUES (?,?,?,?)", batch)
            con.commit()

            batch = []
            for row in rows("trips.txt"):
                tid, rid = (row.get("trip_id") or "").strip(), (row.get("route_id") or "").strip()
                if tid and rid:
                    batch.append((tid, rid, (row.get("service_id") or "").strip(), (row.get("trip_headsign") or "").strip(), (row.get("direction_id") or "").strip()))
                if len(batch) >= 2000:
                    con.executemany("INSERT OR REPLACE INTO trips VALUES (?,?,?,?,?)", batch); batch.clear()
            if batch: con.executemany("INSERT OR REPLACE INTO trips VALUES (?,?,?,?,?)", batch)
            con.commit()

            batch = []
            for row in rows("stops.txt"):
                sid = (row.get("stop_id") or "").strip()
                if not sid: continue
                try:
                    lat = float(row.get("stop_lat") or "")
                    lon = float(row.get("stop_lon") or "")
                except (TypeError, ValueError):
                    lat = lon = None
                name = (row.get("stop_name") or "").strip()
                batch.append((sid, name, normalize_name(name), lat, lon, (row.get("parent_station") or "").strip() or None))
                if len(batch) >= 2000:
                    con.executemany("INSERT OR REPLACE INTO stops VALUES (?,?,?,?,?,?)", batch); batch.clear()
            if batch: con.executemany("INSERT OR REPLACE INTO stops VALUES (?,?,?,?,?,?)", batch)
            con.commit()

            if "calendar.txt" in filemap:
                batch = []
                for row in rows("calendar.txt"):
                    sid = (row.get("service_id") or "").strip()
                    if not sid: continue
                    try:
                        start = datetime.strptime(row.get("start_date", ""), "%Y%m%d").date().isoformat()
                        end = datetime.strptime(row.get("end_date", ""), "%Y%m%d").date().isoformat()
                    except ValueError:
                        continue
                    days = [1 if row.get(k) == "1" else 0 for k in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")]
                    batch.append((sid, start, end, *days))
                    if len(batch) >= 2000:
                        con.executemany("INSERT OR REPLACE INTO calendars VALUES (?,?,?,?,?,?,?,?,?,?)", batch); batch.clear()
                if batch: con.executemany("INSERT OR REPLACE INTO calendars VALUES (?,?,?,?,?,?,?,?,?,?)", batch)
                con.commit()

            if "calendar_dates.txt" in filemap:
                batch = []
                for row in rows("calendar_dates.txt"):
                    sid = (row.get("service_id") or "").strip()
                    try:
                        d = datetime.strptime(row.get("date", ""), "%Y%m%d").date().isoformat()
                        typ = int(row.get("exception_type", "0"))
                    except ValueError:
                        continue
                    if sid: batch.append((sid, d, typ))
                    if len(batch) >= 2000:
                        con.executemany("INSERT OR REPLACE INTO calendar_dates VALUES (?,?,?)", batch); batch.clear()
                if batch: con.executemany("INSERT OR REPLACE INTO calendar_dates VALUES (?,?,?)", batch)
                con.commit()

            # stop_times est le fichier volumineux : import brut en lots, sans requête SQL par ligne.
            batch = []
            for row in rows("stop_times.txt"):
                sid, tid = (row.get("stop_id") or "").strip(), (row.get("trip_id") or "").strip()
                if not sid or not tid: continue
                dep = (row.get("departure_time") or row.get("arrival_time") or "").strip()
                arr = (row.get("arrival_time") or dep).strip()
                if not dep: continue
                try: seq = int(row.get("stop_sequence") or 0)
                except ValueError: seq = 0
                batch.append((sid, tid, arr, dep, seq))
                if len(batch) >= 5000:
                    con.executemany("INSERT INTO raw_stop_times VALUES (?,?,?,?,?)", batch); batch.clear()
            if batch: con.executemany("INSERT INTO raw_stop_times VALUES (?,?,?,?,?)", batch)
            con.commit()

            # Jointure SQL en bloc : beaucoup plus rapide et moins gourmande que des millions de SELECT Python.
            con.create_function("gtfs_seconds", 1, time_seconds, deterministic=True)
            con.execute("""INSERT INTO stop_times
                (stop_id, trip_id, service_id, route_id, arrival_time, departure_time, departure_seconds, stop_sequence)
                SELECT raw.stop_id, raw.trip_id, trips.service_id, trips.route_id, raw.arrival_time, raw.departure_time,
                       gtfs_seconds(raw.departure_time), raw.stop_sequence
                FROM raw_stop_times raw
                JOIN trips ON trips.trip_id=raw.trip_id
                JOIN stops ON stops.stop_id=raw.stop_id
                WHERE trips.route_id IS NOT NULL""")
            con.commit()
            con.execute("DROP TABLE raw_stop_times")
            con.commit()

        # Index construits après l'import pour accélérer l'insertion sur petit serveur.
        con.executescript("""
            CREATE INDEX idx_stoptimes_stop ON stop_times(stop_id, departure_seconds);
            CREATE INDEX idx_stoptimes_trip ON stop_times(trip_id);
            CREATE INDEX idx_stoptimes_service ON stop_times(service_id);
            CREATE INDEX idx_stops_name_norm ON stops(name_norm);
            CREATE INDEX idx_calendar_dates_date ON calendar_dates(service_date, service_id);
            CREATE INDEX idx_trips_route ON trips(route_id);
            CREATE TABLE stop_routes AS SELECT DISTINCT stop_id, route_id FROM stop_times;
            CREATE INDEX idx_stop_routes_stop ON stop_routes(stop_id, route_id);
        """)
        counts = {
            "stops": con.execute("SELECT COUNT(*) FROM stops").fetchone()[0],
            "routes": con.execute("SELECT COUNT(*) FROM routes").fetchone()[0],
            "trips": con.execute("SELECT COUNT(*) FROM trips").fetchone()[0],
            "stop_times": con.execute("SELECT COUNT(*) FROM stop_times").fetchone()[0],
        }
        loaded_iso = datetime.now(timezone.utc).isoformat()
        con.executemany("INSERT INTO meta(key,value) VALUES (?,?)", [("loaded_at", loaded_iso), ("source", SOURCE_STATIC)])
        con.commit()
        con.execute("PRAGMA optimize")
        return {"counts": counts, "loaded_at": loaded_iso}
    finally:
        con.close()


async def load_static_gtfs(force_refresh: bool = False) -> dict:
    """Assure que la base SQLite existe; verrou empêche plusieurs imports simultanés."""
    global _static_loaded_at, _static_loaded_iso, _static_counts
    now = time.time()
    if table_exists() and not force_refresh and now - _static_loaded_at < STATIC_CACHE_TTL_SECONDS:
        return {"source": SOURCE_STATIC, "loaded_at": _static_loaded_iso or "unknown", "counts": _static_counts or _read_counts()}

    async with _static_lock:
        now = time.time()
        if table_exists() and not force_refresh and now - _static_loaded_at < STATIC_CACHE_TTL_SECONDS:
            return {"source": SOURCE_STATIC, "loaded_at": _static_loaded_iso or "unknown", "counts": _static_counts or _read_counts()}
        if not API_KEY or not API_BASE:
            raise HTTPException(status_code=503, detail="Configuration Render incomplète : BM_API_KEY ou BM_API_BASE manquante.")
        TMP_DIR.mkdir(parents=True, exist_ok=True)
        started = time.time()
        try:
            # Téléchargement en chunks vers disque : response.content ne garde pas 94 Mo en RAM.
            async with httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=20.0), follow_redirects=True) as client:
                async with client.stream("GET", API_BASE + STATIC_URL_PATH, headers={"Ocp-Apim-Subscription-Key": API_KEY, "Accept": "application/zip, application/octet-stream"}) as response:
                    if response.status_code in (401, 403):
                        raise HTTPException(status_code=502, detail="Accès refusé au GTFS statique par Belgian Mobility. Vérifie les droits de la clé API.")
                    if response.status_code >= 400:
                        raise HTTPException(status_code=502, detail=f"Belgian Mobility a répondu HTTP {response.status_code} pour le GTFS statique.")
                    with open(ZIP_PATH, "wb") as out:
                        async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                            if chunk: out.write(chunk)
            print(f"GTFS statique téléchargé : {ZIP_PATH.stat().st_size} octets en {time.time()-started:.1f}s")
            # Import synchrone vers SQLite : aucune accumulation des stop_times en mémoire.
            info = await asyncio.to_thread(_download_and_build_db, ZIP_PATH, BUILD_DB_PATH)
            # Remplacement atomique de la base uniquement après import réussi.
            os.replace(BUILD_DB_PATH, DB_PATH)
            # L'archive n'est plus utile après l'import : libère l'espace disque temporaire.
            try:
                ZIP_PATH.unlink(missing_ok=True)
            except OSError:
                pass
            _static_loaded_at = time.time()
            _static_loaded_iso = info["loaded_at"]
            _static_counts = info["counts"]
            print(f"GTFS SQLite prêt : {info['counts']} ; base={DB_PATH.stat().st_size} octets ; durée totale={time.time()-started:.1f}s")
            return {"source": SOURCE_STATIC, **info}
        except HTTPException:
            raise
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Délai dépassé lors du téléchargement GTFS statique.")
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail=f"Erreur réseau lors du téléchargement GTFS statique : {type(exc).__name__}.")
        except Exception as exc:
            print(f"Erreur import GTFS SQLite : {type(exc).__name__}: {exc}")
            if BUILD_DB_PATH.exists():
                try: BUILD_DB_PATH.unlink()
                except OSError: pass
            try:
                ZIP_PATH.unlink(missing_ok=True)
            except OSError:
                pass
            raise HTTPException(status_code=502, detail=f"Le GTFS statique n'a pas pu être importé dans SQLite ({type(exc).__name__}). Consulte les logs Render.")


def _read_counts() -> dict:
    if not table_exists(): return {"stops": 0, "routes": 0, "trips": 0, "stop_times": 0}
    try:
        with db_connect() as con:
            return {name: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for name, table in [("stops", "stops"), ("routes", "routes"), ("trips", "trips"), ("stop_times", "stop_times")]}
    except sqlite3.Error:
        return {"stops": 0, "routes": 0, "trips": 0, "stop_times": 0}


def _route_objects_for_stops(con: sqlite3.Connection, stop_ids: list[str]) -> dict:
    result = {sid: [] for sid in stop_ids}
    if not stop_ids: return result
    # Groupes de paramètres limités pour respecter la limite SQLite.
    for offset in range(0, len(stop_ids), 500):
        chunk = stop_ids[offset:offset+500]
        placeholders = ",".join("?" for _ in chunk)
        query = f"""SELECT sr.stop_id, r.route_id, r.short_name, r.long_name, r.route_type
                    FROM stop_routes sr JOIN routes r ON r.route_id=sr.route_id
                    WHERE sr.stop_id IN ({placeholders}) ORDER BY sr.stop_id, r.route_id"""
        for row in con.execute(query, chunk):
            result[row["stop_id"]].append({"route_id": row["route_id"], "short_name": row["short_name"], "long_name": row["long_name"], "route_type": row["route_type"]})
    return result


def _stop_objects(con: sqlite3.Connection, rows: list[sqlite3.Row]) -> list[dict]:
    ids = [r["stop_id"] for r in rows]
    route_map = _route_objects_for_stops(con, ids)
    output = []
    for row in rows:
        routes = route_map.get(row["stop_id"], [])
        output.append({
            "stop_id": row["stop_id"], "name": row["name"], "lat": row["lat"], "lon": row["lon"],
            "parent_station": row["parent_station"], "routes": routes,
            "route_ids": [r["route_id"] for r in routes],
        })
    return output


def _service_is_active(con: sqlite3.Connection, service_id: str, service_date: date) -> bool:
    ds = service_date.isoformat()
    exception = con.execute("SELECT exception_type FROM calendar_dates WHERE service_id=? AND service_date=?", (service_id, ds)).fetchone()
    if exception:
        return exception[0] == 1
    cal = con.execute("SELECT * FROM calendars WHERE service_id=?", (service_id,)).fetchone()
    if not cal or not (cal["start_date"] <= ds <= cal["end_date"]):
        return False
    keys = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
    return bool(cal[keys[service_date.weekday()]])


@app.get("/api/tec/static/summary")
async def static_summary(refresh: bool = False):
    data = await load_static_gtfs(force_refresh=refresh)
    return {"source": data["source"], "loaded_at": data["loaded_at"], "counts": data["counts"]}


@app.get("/api/tec/static/stops")
async def static_stops(q: str = "", limit: int = 5000):
    data = await load_static_gtfs()
    limit = max(1, min(limit, 20000))
    needle = normalize_name(q)
    with db_connect() as con:
        if needle:
            rows = con.execute("SELECT * FROM stops WHERE name_norm LIKE ? OR lower(stop_id) LIKE ? ORDER BY name_norm, stop_id LIMIT ?", (f"%{needle}%", f"%{q.strip().lower()}%", limit)).fetchall()
        else:
            rows = con.execute("SELECT * FROM stops ORDER BY name_norm, stop_id LIMIT ?", (limit,)).fetchall()
        stops = _stop_objects(con, rows)
    return {"source": data["source"], "loaded_at": data["loaded_at"], "count": len(stops), "stops": stops}


@app.get("/api/tec/static/passages")
async def static_passages(q: str = "", stop_id: str = "", date_str: str = "", after: str = "", limit: int = 12):
    """Passages théoriques du GTFS officiel, filtrés avec le calendrier de service."""
    data = await load_static_gtfs()
    local_now = datetime.now(BRUSSELS)
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else local_now.date()
    except ValueError:
        raise HTTPException(status_code=400, detail="date_str doit être au format AAAA-MM-JJ.")
    after_seconds = None
    if after:
        try:
            parts = after.split(":")
            if len(parts) not in (2, 3): raise ValueError
            after_seconds = int(parts[0]) * 3600 + int(parts[1]) * 60 + (int(parts[2]) if len(parts) == 3 else 0)
        except (ValueError, IndexError):
            raise HTTPException(status_code=400, detail="after doit être au format HH:MM ou HH:MM:SS.")
    limit = max(1, min(limit, 100))

    with db_connect() as con:
        if stop_id:
            candidate_rows = con.execute("SELECT * FROM stops WHERE stop_id=?", (stop_id,)).fetchall()
        elif q.strip():
            needle = normalize_name(q)
            candidate_rows = con.execute("SELECT * FROM stops WHERE name_norm=? ORDER BY stop_id LIMIT 10", (needle,)).fetchall()
            if not candidate_rows:
                candidate_rows = con.execute("SELECT * FROM stops WHERE name_norm LIKE ? ORDER BY name_norm, stop_id LIMIT 10", (f"%{needle}%",)).fetchall()
        else:
            raise HTTPException(status_code=400, detail="Fournis stop_id ou q avec le nom de l'arrêt.")

        if not candidate_rows:
            return {"source": data["source"], "date": target_date.isoformat(), "count": 0, "stops": [], "passages": [], "message": "Aucun arrêt officiel correspondant. Essaie un nom plus court."}

        candidates = _stop_objects(con, candidate_rows)
        passages = []
        for stop in candidates:
            sid = stop["stop_id"]
            rows = con.execute("""SELECT st.*, t.headsign, r.short_name, r.long_name
                                 FROM stop_times st
                                 LEFT JOIN trips t ON t.trip_id=st.trip_id
                                 LEFT JOIN routes r ON r.route_id=st.route_id
                                 WHERE st.stop_id=? ORDER BY st.departure_seconds, r.short_name LIMIT 20000""", (sid,)).fetchall()
            for item in rows:
                raw_seconds = item["departure_seconds"]
                if raw_seconds >= 86400:
                    service_date = target_date - timedelta(days=1)
                    dep_seconds = raw_seconds - 86400
                    if after_seconds is not None and after_seconds >= 6 * 3600:
                        continue
                else:
                    service_date = target_date
                    dep_seconds = raw_seconds
                if not _service_is_active(con, item["service_id"], service_date):
                    continue
                if after_seconds is not None and dep_seconds < after_seconds:
                    continue
                line = item["short_name"] or item["long_name"] or item["route_id"]
                passages.append({
                    "stop_id": sid, "stop_name": stop["name"], "trip_id": item["trip_id"],
                    "service_id": item["service_id"], "route_id": item["route_id"], "line": line,
                    "destination": item["headsign"] or item["long_name"] or "Destination non précisée",
                    "arrival_time": display_gtfs_time(item["arrival_time"]),
                    "departure_time": display_gtfs_time(str(dep_seconds // 3600).zfill(2) + ":" + str((dep_seconds % 3600) // 60).zfill(2) + ":" + str(dep_seconds % 60).zfill(2)),
                    "departure_seconds": dep_seconds, "status": "Horaire théorique GTFS", "realtime_delay_seconds": None,
                })
        passages.sort(key=lambda x: (x["departure_seconds"], str(x["line"])))
        return {
            "source": data["source"], "loaded_at": data["loaded_at"], "date": target_date.isoformat(),
            "count": min(len(passages), limit), "stops": candidates, "passages": passages[:limit],
            "realtime_join": "not_applied",
            "note": "Horaires théoriques officiels. Les retards en temps réel ne sont pas indiqués tant que la correspondance des identifiants GTFS-RT n'est pas confirmée.",
        }
