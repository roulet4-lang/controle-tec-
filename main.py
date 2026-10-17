
import os
import csv
import io
import time
import zipfile
import logging
import unicodedata
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from google.transit import gtfs_realtime_pb2


# ============================================================
# CONFIGURATION
# ============================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tec-service-controle")

API_KEY = os.getenv("BM_API_KEY", "").strip()
API_BASE = os.getenv("BM_API_BASE", "").strip().rstrip("/")

STATIC_API_BASE = os.getenv(
    "BM_STATIC_API_BASE",
    "https://opendata-discovery-gtfs-static.api.production.belgianmobility.io",
).strip().rstrip("/")

STATIC_CACHE_TTL_SECONDS = int(
    os.getenv("STATIC_CACHE_TTL_SECONDS", "21600")
)

_static_cache = {"loaded_at": 0.0, "data": None}
BRUSSELS = ZoneInfo("Europe/Brussels")

FEEDS = {
    "vehicles": "/api/gtfs/feed/tec/rt/vehicle-position",
    "trips": "/api/gtfs/feed/tec/rt/trip-update",
    "alerts": "/api/gtfs/feed/tec/rt/alert",
}

STATIC_FEED_PATH = "/api/gtfs/feed/tec/static"

app = FastAPI(
    title="TEC Service Contrôle API",
    version="1.4.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://roulet4-lang.github.io",
    ],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ============================================================
# OUTILS COMMUNS
# ============================================================

def require_api_config():
    if not API_KEY:
        raise HTTPException(
            status_code=500,
            detail="Variable BM_API_KEY absente sur le serveur.",
        )

    if not API_BASE:
        raise HTTPException(
            status_code=500,
            detail="Variable BM_API_BASE absente sur le serveur.",
        )


def api_headers():
    return {
        "Ocp-Apim-Subscription-Key": API_KEY,
        "Accept": "application/x-protobuf",
    }


def normalize_name(value):
    value = str(value or "").strip().lower()
    value = unicodedata.normalize("NFKD", value)
    return "".join(
        char for char in value
        if not unicodedata.combining(char)
    )


def time_seconds(value):
    """Convertit un horaire GTFS HH:MM:SS en secondes."""
    try:
        parts = str(value).split(":")
        if len(parts) != 3:
            return None

        hours, minutes, seconds = map(int, parts)
        return hours * 3600 + minutes * 60 + seconds
    except (ValueError, TypeError):
        return None


def display_gtfs_time(value):
    """Conserve les heures GTFS supérieures à 24 h."""
    seconds = time_seconds(value)
    if seconds is None:
        return value

    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def read_csv_from_zip(archive, filename):
    """Lit un CSV GTFS en UTF-8 avec tolérance aux caractères invalides."""
    with archive.open(filename) as file:
        content = file.read().decode("utf-8-sig", errors="replace")

    return list(csv.DictReader(io.StringIO(content)))


def find_gtfs_files(archive):
    """Indexe les fichiers GTFS même s'ils sont dans un sous-dossier."""
    result = {}

    for name in archive.namelist():
        if name.endswith("/"):
            continue

        basename = name.rsplit("/", 1)[-1].lower()

        if basename.endswith(".txt"):
            result[basename] = name

    return result


def service_active(service_id, service_date, calendars, exceptions):
    """
    Détermine si un service GTFS fonctionne à une date donnée.
    calendar_dates.txt peut ajouter ou supprimer un service.
    """
    date_key = service_date.strftime("%Y%m%d")

    exception = exceptions.get((service_id, date_key))
    if exception is not None:
        return exception == "1"

    calendar = calendars.get(service_id)
    if not calendar:
        return False

    if not (
        calendar.get("start_date", "") <= date_key
        <= calendar.get("end_date", "")
    ):
        return False

    weekday = service_date.weekday()
    weekday_fields = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]

    return calendar.get(weekday_fields[weekday], "0") == "1"


# ============================================================
# FLUX TEMPS RÉEL GTFS-RT
# ============================================================

async def fetch_feed(path):
    require_api_config()

    url = API_BASE + path

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                url,
                params={"format": "protobuf"},
                headers=api_headers(),
            )

        if response.status_code in (401, 403):
            raise HTTPException(
                status_code=response.status_code,
                detail="Accès refusé par l'API de mobilité. Vérifier la clé API.",
            )

        if response.status_code == 404:
            raise HTTPException(
                status_code=502,
                detail=f"Flux temps réel introuvable : {path}",
            )

        if response.status_code == 429:
            raise HTTPException(
                status_code=429,
                detail="Limite de requêtes de l'API atteinte.",
            )

        response.raise_for_status()

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        timestamp = None
        if feed.header.HasField("timestamp"):
            timestamp = str(feed.header.timestamp)

        return {
            "ok": True,
            "count": len(feed.entity),
            "feed_timestamp": timestamp,
            "entities": [
                entity.SerializeToString().hex()
                for entity in feed.entity
            ],
        }

    except HTTPException:
        raise
    except httpx.TimeoutException:
        logger.exception("Délai dépassé sur le flux %s", path)
        raise HTTPException(
            status_code=504,
            detail=f"Le flux temps réel a dépassé le délai autorisé : {path}",
        )
    except httpx.HTTPStatusError as exc:
        logger.error(
            "Erreur HTTP flux %s : statut=%s",
            path,
            exc.response.status_code,
        )
        raise HTTPException(
            status_code=502,
            detail=f"Erreur HTTP du fournisseur sur le flux {path}.",
        )
    except Exception as exc:
        logger.exception("Erreur flux temps réel %s", path)
        raise HTTPException(
            status_code=502,
            detail=f"Impossible de lire le flux {path} ({type(exc).__name__}).",
        )


# ============================================================
# TÉLÉCHARGEMENT ET ANALYSE DU GTFS STATIQUE
# ============================================================

async def load_static_gtfs(force_refresh=False):
    now = time.time()

    if (
        not force_refresh
        and _static_cache["data"] is not None
        and now - _static_cache["loaded_at"] < STATIC_CACHE_TTL_SECONDS
    ):
        return _static_cache["data"]

    require_api_config()

    url = STATIC_API_BASE + STATIC_FEED_PATH
    headers = {
        "Ocp-Apim-Subscription-Key": API_KEY,
        "Accept": "application/zip, application/octet-stream",
    }

    logger.info("Téléchargement GTFS statique depuis %s", STATIC_API_BASE)
    started = time.monotonic()

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(120.0, connect=20.0),
            follow_redirects=True,
        ) as client:
            response = await client.get(url, headers=headers)

        logger.info(
            "Réponse GTFS statique : status=%s, content_type=%s, "
            "taille=%s octets, durée=%.2fs",
            response.status_code,
            response.headers.get("content-type", "inconnu"),
            len(response.content),
            time.monotonic() - started,
        )

        if response.status_code in (401, 403):
            raise HTTPException(
                status_code=response.status_code,
                detail=(
                    "Accès refusé au GTFS statique. Vérifier la clé API "
                    "et les droits associés."
                ),
            )

        if response.status_code == 404:
            raise HTTPException(
                status_code=502,
                detail=f"Endpoint GTFS statique introuvable : {url}",
            )

        if response.status_code == 429:
            raise HTTPException(
                status_code=429,
                detail="Limite de requêtes atteinte sur le GTFS statique.",
            )

        if response.status_code >= 400:
            raise HTTPException(
                status_code=502,
                detail=(
                    "Erreur HTTP du fournisseur GTFS statique : "
                    f"statut {response.status_code}."
                ),
            )

        if not response.content:
            raise HTTPException(
                status_code=502,
                detail="Le fournisseur a renvoyé un fichier GTFS vide.",
            )

        try:
            archive = zipfile.ZipFile(io.BytesIO(response.content))
        except zipfile.BadZipFile:
            content_type = response.headers.get("content-type", "inconnu")
            logger.error(
                "Le contenu reçu n'est pas un ZIP. content_type=%s",
                content_type,
            )
            raise HTTPException(
                status_code=502,
                detail=(
                    "Le fournisseur n'a pas renvoyé un ZIP GTFS valide. "
                    f"Type de contenu reçu : {content_type}."
                ),
            )

        with archive:
            files = find_gtfs_files(archive)

            required = [
                "stops.txt",
                "routes.txt",
                "trips.txt",
                "stop_times.txt",
            ]

            missing = [name for name in required if name not in files]

            if missing:
                logger.error(
                    "Fichiers GTFS absents : %s. Fichiers trouvés : %s",
                    missing,
                    list(files.keys()),
                )
                raise HTTPException(
                    status_code=502,
                    detail=(
                        "Le ZIP GTFS ne contient pas les fichiers requis : "
                        + ", ".join(missing)
                    ),
                )

            stops_rows = read_csv_from_zip(archive, files["stops.txt"])
            routes_rows = read_csv_from_zip(archive, files["routes.txt"])
            trips_rows = read_csv_from_zip(archive, files["trips.txt"])
            stop_times_rows = read_csv_from_zip(
                archive, files["stop_times.txt"]
            )

            calendars = {}
            exceptions = {}

            if "calendar.txt" in files:
                for row in read_csv_from_zip(
                    archive, files["calendar.txt"]
                ):
                    calendars[row.get("service_id", "")] = row

            if "calendar_dates.txt" in files:
                for row in read_csv_from_zip(
                    archive, files["calendar_dates.txt"]
                ):
                    exceptions[
                        (
                            row.get("service_id", ""),
                            row.get("date", ""),
                        )
                    ] = row.get("exception_type", "")

            stops = {
                row.get("stop_id", ""): row
                for row in stops_rows
                if row.get("stop_id")
            }

            routes = {
                row.get("route_id", ""): row
                for row in routes_rows
                if row.get("route_id")
            }

            trips = {
                row.get("trip_id", ""): row
                for row in trips_rows
                if row.get("trip_id")
            }

            by_stop = {}

            for row in stop_times_rows:
                stop_id = row.get("stop_id", "")
                trip_id = row.get("trip_id", "")

                if not stop_id or not trip_id:
                    continue

                trip = trips.get(trip_id)
                if not trip:
                    continue

                route_id = trip.get("route_id", "")
                route = routes.get(route_id, {})

                passage = {
                    "stop_id": stop_id,
                    "stop_name": stops.get(stop_id, {}).get(
                        "stop_name", ""
                    ),
                    "trip_id": trip_id,
                    "route_id": route_id,
                    "route_short_name": route.get(
                        "route_short_name", ""
                    ),
                    "route_long_name": route.get(
                        "route_long_name", ""
                    ),
                    "service_id": trip.get("service_id", ""),
                    "trip_headsign": trip.get("trip_headsign", ""),
                    "direction_id": trip.get("direction_id", ""),
                    "stop_sequence": row.get("stop_sequence", ""),
                    "arrival_time": display_gtfs_time(
                        row.get("arrival_time", "")
                    ),
                    "departure_time": display_gtfs_time(
                        row.get("departure_time", "")
                    ),
                    "_departure_seconds": time_seconds(
                        row.get("departure_time", "")
                    ),
                }

                by_stop.setdefault(stop_id, []).append(passage)

            for passages in by_stop.values():
                passages.sort(
                    key=lambda item: (
                        item["_departure_seconds"]
                        if item["_departure_seconds"] is not None
                        else 10**12
                    )
                )

            data = {
                "source": "GTFS statique",
                "loaded_at": datetime.now(BRUSSELS).isoformat(),
                "stops": stops,
                "routes": routes,
                "trips": trips,
                "by_stop": by_stop,
                "calendars": calendars,
                "exceptions": exceptions,
            }

        _static_cache["data"] = data
        _static_cache["loaded_at"] = time.time()

        logger.info(
            "GTFS statique chargé en %.2fs : %s arrêts, %s lignes, "
            "%s voyages, %s arrêts-horaires",
            time.monotonic() - started,
            len(data["stops"]),
            len(data["routes"]),
            len(data["trips"]),
            sum(len(items) for items in data["by_stop"].values()),
        )

        return data

    except HTTPException:
        raise
    except httpx.TimeoutException:
        logger.exception("Délai dépassé pendant le téléchargement GTFS")
        raise HTTPException(
            status_code=504,
            detail="Le téléchargement du GTFS statique a dépassé 120 secondes.",
        )
    except httpx.HTTPError as exc:
        logger.exception("Erreur réseau GTFS statique")
        raise HTTPException(
            status_code=502,
            detail=f"Erreur réseau GTFS statique ({type(exc).__name__}).",
        )
    except Exception as exc:
        logger.exception("Échec de chargement ou d'analyse du GTFS statique")
        raise HTTPException(
            status_code=502,
            detail=(
                "Le GTFS statique n'a pas pu être analysé "
                f"({type(exc).__name__}). Consulter les logs du serveur."
            ),
        )


# ============================================================
# ROUTES GÉNÉRALES
# ============================================================

@app.get("/")
async def root():
    return {
        "service": "TEC Service Contrôle API",
        "version": app.version,
        "status": "online",
        "realtime_feeds": list(FEEDS.keys()),
        "static_endpoints": [
            "/api/tec/static/summary",
            "/api/tec/static/stops",
            "/api/tec/static/passages",
        ],
    }


@app.get("/health")
async def health():
    return {"ok": True, "status": "online"}


@app.get("/api/tec/connection-test")
async def connection_test():
    results = {}

    for name, path in FEEDS.items():
        try:
            result = await fetch_feed(path)
            results[name] = {
                "ok": True,
                "count": result["count"],
                "feed_timestamp": result["feed_timestamp"],
            }
        except HTTPException as exc:
            results[name] = {
                "ok": False,
                "status_code": exc.status_code,
                "error": exc.detail,
            }

    return {
        "all_ok": all(item["ok"] for item in results.values()),
        "checked_at": datetime.now(BRUSSELS).isoformat(),
        "feeds": results,
    }


# ============================================================
# ROUTES TEMPS RÉEL
# ============================================================

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
# ROUTES GTFS STATIQUE
# ============================================================

@app.get("/api/tec/static/summary")
async def static_summary(refresh: bool = False):
    data = await load_static_gtfs(force_refresh=refresh)

    return {
        "ok": True,
        "source": data["source"],
        "loaded_at": data["loaded_at"],
        "counts": {
            "stops": len(data["stops"]),
            "routes": len(data["routes"]),
            "trips": len(data["trips"]),
            "stops_with_passages": len(data["by_stop"]),
            "calendar_services": len(data["calendars"]),
            "calendar_exceptions": len(data["exceptions"]),
        },
    }


@app.get("/api/tec/static/stops")
async def static_stops(
    q: str = "",
    limit: int = Query(default=100, ge=1, le=20000),
):
    data = await load_static_gtfs()
    query = normalize_name(q)

    results = []

    for stop_id, stop in data["stops"].items():
        name = stop.get("stop_name", "")

        if query and query not in normalize_name(name) and query not in normalize_name(stop_id):
            continue

        results.append({
            "stop_id": stop_id,
            "stop_name": name,
            "stop_code": stop.get("stop_code", ""),
            "stop_desc": stop.get("stop_desc", ""),
            "stop_lat": stop.get("stop_lat", ""),
            "stop_lon": stop.get("stop_lon", ""),
            "location_type": stop.get("location_type", ""),
            "parent_station": stop.get("parent_station", ""),
            "wheelchair_boarding": stop.get("wheelchair_boarding", ""),
        })

        if len(results) >= limit:
            break

    return {
        "ok": True,
        "count": len(results),
        "stops": results,
    }


@app.get("/api/tec/static/passages")
async def static_passages(
    stop_id: str = "",
    q: str = "",
    date_str: str = "",
    after: str = "",
    limit: int = Query(default=100, ge=1, le=2000),
):
    data = await load_static_gtfs()

    if not stop_id and not q:
        raise HTTPException(
            status_code=400,
            detail="Fournir stop_id ou q pour rechercher un arrêt.",
        )

    if date_str:
        try:
            service_date = date.fromisoformat(date_str)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="date_str doit respecter le format AAAA-MM-JJ.",
            )
    else:
        service_date = datetime.now(BRUSSELS).date()

    query = normalize_name(q)

    matching_stop_ids = []

    if stop_id:
        if stop_id in data["stops"]:
            matching_stop_ids = [stop_id]
    else:
        for sid, stop in data["stops"].items():
            if query in normalize_name(stop.get("stop_name", "")):
                matching_stop_ids.append(sid)

    if not matching_stop_ids:
        return {
            "ok": True,
            "date": service_date.isoformat(),
            "count": 0,
            "passages": [],
            "realtime_join": "not_applied",
        }

    after_seconds = time_seconds(after) if after else None
    results = []

    for sid in matching_stop_ids:
        for passage in data["by_stop"].get(sid, []):
            service_id = passage.get("service_id", "")

            if not service_active(
                service_id,
                service_date,
                data["calendars"],
                data["exceptions"],
            ):
                continue

            departure_seconds = passage.get("_departure_seconds")

            if (
                after_seconds is not None
                and departure_seconds is not None
                and departure_seconds < after_seconds
            ):
                continue

            item = {
                key: value
                for key, value in passage.items()
                if not key.startswith("_")
            }
            results.append(item)

    results.sort(
        key=lambda item: (
            time_seconds(item.get("departure_time", ""))
            if time_seconds(item.get("departure_time", "")) is not None
            else 10**12
        )
    )

    results = results[:limit]

    return {
        "ok": True,
        "date": service_date.isoformat(),
        "count": len(results),
        "passages": results,
        "realtime_join": "not_applied",
    }
