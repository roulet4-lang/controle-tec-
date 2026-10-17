
import os
import csv
import io
import time
import zipfile
import unicodedata
from datetime import datetime, timezone, date, timedelta
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict


# ============================================================
# CONFIGURATION
# ============================================================

app = FastAPI(
    title="TEC Service Contrôle API",
    description="Passerelle sécurisée vers les flux officiels TEC.",
    version="1.3.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://roulet4-lang.github.io",
    ],
    allow_methods=["GET"],
    allow_headers=["*"],
)

API_KEY = os.getenv("BM_API_KEY", "").strip()
API_BASE = os.getenv("BM_API_BASE", "").strip().rstrip("/")

# Cache mémoire du GTFS statique : le flux officiel est mis à jour quotidiennement.
STATIC_CACHE_TTL_SECONDS = int(os.getenv("STATIC_CACHE_TTL_SECONDS", "21600"))
_static_cache = {"loaded_at": 0.0, "data": None}
BRUSSELS = ZoneInfo("Europe/Brussels")

FEEDS = {
    "vehicles": "/api/gtfs/feed/tec/rt/vehicle-position",
    "trips": "/api/gtfs/feed/tec/rt/trip-update",
    "alerts": "/api/gtfs/feed/tec/rt/alert",
}


# ============================================================
# CONNEXION AUX FLUX BELGIAN MOBILITY
# ============================================================

async def fetch_feed(path: str) -> dict:
    if not API_KEY or not API_BASE:
        raise HTTPException(
            status_code=503,
            detail=(
                "Configuration Render incomplète : "
                "BM_API_KEY ou BM_API_BASE manquante."
            ),
        )

    if not API_BASE.startswith("https://"):
        raise HTTPException(
            status_code=503,
            detail="BM_API_BASE doit commencer par https://",
        )

    url = API_BASE + path

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                url,
                params={"format": "protobuf"},
                headers={
                    "Ocp-Apim-Subscription-Key": API_KEY,
                    "Accept": "application/x-protobuf",
                },
            )

    except httpx.TimeoutException:
        raise HTTPException(
            status_code=504,
            detail="Délai dépassé lors de la connexion à Belgian Mobility.",
        )

    except httpx.HTTPError as exc:
        # Ne jamais inclure les en-têtes ou la clé API dans les erreurs.
        raise HTTPException(
            status_code=502,
            detail=f"Erreur réseau vers Belgian Mobility : {type(exc).__name__}.",
        )

    # Diagnostic des erreurs renvoyées par l'API distante.
    if response.status_code in (401, 403):
        raise HTTPException(
            status_code=502,
            detail=(
                f"Accès refusé par Belgian Mobility (HTTP "
                f"{response.status_code}). Vérifie l'abonnement, "
                "la clé et les autorisations."
            ),
        )

    if response.status_code == 404:
        raise HTTPException(
            status_code=502,
            detail=(
                f"Endpoint introuvable (HTTP 404) : {path}. "
                "Vérifie l'URL de base, le chemin et les paramètres "
                "dans le portail Belgian Mobility."
            ),
        )

    if response.status_code == 429:
        raise HTTPException(
            status_code=503,
            detail="Limite de requêtes Belgian Mobility atteinte.",
        )

    if response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail=(
                f"Belgian Mobility a répondu HTTP "
                f"{response.status_code} pour {path}."
            ),
        )

    # Décodage du flux GTFS-RT au format Protocol Buffers.
    feed = gtfs_realtime_pb2.FeedMessage()

    try:
        feed.ParseFromString(response.content)
    except Exception:
        raise HTTPException(
            status_code=502,
            detail=(
                f"Réponse reçue pour {path}, mais impossible "
                "de la décoder en GTFS-Realtime protobuf. "
                "Vérifie le format réellement renvoyé par l'API."
            ),
        )

    data = MessageToDict(
        feed,
        preserving_proto_field_name=True,
        use_integers_for_enums=True,
    )

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


# ============================================================
# ROUTES PRINCIPALES
# ============================================================

@app.get("/")
def home():
    return {
        "service": "TEC Service Contrôle API",
        "version": "1.1.0",
        "status": "online",
        "health": "/health",
        "docs": "/docs",
        "connection_test": "/api/tec/connection-test",
    }


@app.get("/health")
def health():
    # Vérifie uniquement la configuration locale,
    # pas la connexion réelle à Belgian Mobility.
    return {
        "status": "ok",
        "api_key_configured": bool(API_KEY),
        "api_base_configured": bool(API_BASE),
    }


# ============================================================
# TEST DES TROIS FLUX
# ============================================================

@app.get("/api/tec/connection-test")
async def connection_test():
    results = {}

    for name, path in FEEDS.items():
        try:
            data = await fetch_feed(path)

            results[name] = {
                "ok": True,
                "count": data["count"],
                "feed_timestamp": data["feed_timestamp"],
            }

        except HTTPException as exc:
            results[name] = {
                "ok": False,
                "error": exc.detail,
            }

    return {
        "all_ok": all(item["ok"] for item in results.values()),
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "feeds": results,
    }


# ============================================================
# VEHICULES EN TEMPS REEL
# ============================================================

@app.get("/api/tec/vehicles")
async def vehicles():
    return await fetch_feed(FEEDS["vehicles"])


# ============================================================
# MISES A JOUR DES TRAJETS
# ============================================================

@app.get("/api/tec/trips")
async def trips():
    return await fetch_feed(FEEDS["trips"])


# ============================================================
# ALERTES ET PERTURBATIONS
# ============================================================

@app.get("/api/tec/alerts")
async def alerts():
    return await fetch_feed(FEEDS["alerts"])


# ============================================================
# GTFS STATIQUE OFFICIEL : ARRETS ET LIGNES REELLEMENT DESSERVIES
# ============================================================

async def load_static_gtfs(force_refresh: bool = False) -> dict:
    """Télécharge et indexe le GTFS statique officiel TEC."""
    now = time.time()
    cached = _static_cache.get("data")
    if cached and not force_refresh and now - _static_cache["loaded_at"] < STATIC_CACHE_TTL_SECONDS:
        return cached

    if not API_KEY or not API_BASE:
        raise HTTPException(status_code=503, detail="Configuration Render incomplète : BM_API_KEY ou BM_API_BASE manquante.")

    try:
        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
            response = await client.get(
                API_BASE + "/api/gtfs/feed/tec/static",
                headers={"Ocp-Apim-Subscription-Key": API_KEY, "Accept": "application/zip, application/octet-stream"},
            )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Erreur réseau lors du téléchargement GTFS statique : {type(exc).__name__}.")

    if response.status_code in (401, 403):
        raise HTTPException(status_code=502, detail="Accès refusé au GTFS statique par Belgian Mobility. Vérifie les droits de la clé API.")
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Belgian Mobility a répondu HTTP {response.status_code} pour le GTFS statique.")

    try:
        archive = zipfile.ZipFile(io.BytesIO(response.content))
        # GTFS archives may place files inside a root folder.
        filemap = {name.rsplit('/', 1)[-1]: name for name in archive.namelist() if '/' not in name or name.endswith('.txt')}
        required = {"stops.txt", "routes.txt", "trips.txt", "stop_times.txt"}
        if not required.issubset(filemap):
            raise ValueError("fichiers GTFS obligatoires absents")

        def rows(filename):
            with archive.open(filemap[filename]) as f:
                wrapper = io.TextIOWrapper(f, encoding="utf-8-sig", newline="")
                yield from csv.DictReader(wrapper)

        routes = {}
        for r in rows("routes.txt"):
            rid = (r.get("route_id") or "").strip()
            if rid:
                routes[rid] = {
                    "route_id": rid,
                    "short_name": (r.get("route_short_name") or "").strip(),
                    "long_name": (r.get("route_long_name") or "").strip(),
                    "route_type": (r.get("route_type") or "").strip(),
                }

        trips = {}
        for t in rows("trips.txt"):
            tid = (t.get("trip_id") or "").strip()
            rid = (t.get("route_id") or "").strip()
            if tid and rid:
                trips[tid] = {
                    "trip_id": tid,
                    "route_id": rid,
                    "service_id": (t.get("service_id") or "").strip(),
                    "headsign": (t.get("trip_headsign") or "").strip(),
                    "direction_id": (t.get("direction_id") or "").strip(),
                }

        stops = {}
        for st in rows("stops.txt"):
            sid = (st.get("stop_id") or "").strip()
            if not sid:
                continue
            try:
                lat = float(st.get("stop_lat", ""))
                lon = float(st.get("stop_lon", ""))
            except (TypeError, ValueError):
                lat = lon = None
            stops[sid] = {
                "stop_id": sid,
                "name": (st.get("stop_name") or "").strip(),
                "lat": lat,
                "lon": lon,
                "parent_station": (st.get("parent_station") or "").strip() or None,
                "route_ids": set(),
            }

        # Service calendar and calendar exceptions
        calendars = {}
        if "calendar.txt" in filemap:
            for c in rows("calendar.txt"):
                sid = (c.get("service_id") or "").strip()
                if not sid:
                    continue
                try:
                    start_date = datetime.strptime(c.get("start_date", ""), "%Y%m%d").date()
                    end_date = datetime.strptime(c.get("end_date", ""), "%Y%m%d").date()
                except ValueError:
                    continue
                calendars[sid] = {
                    "start": start_date,
                    "end": end_date,
                    "weekdays": {i for i, key in enumerate(("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")) if c.get(key) == "1"},
                }
        exceptions = {}
        if "calendar_dates.txt" in filemap:
            for c in rows("calendar_dates.txt"):
                sid = (c.get("service_id") or "").strip()
                try:
                    d = datetime.strptime(c.get("date", ""), "%Y%m%d").date()
                    typ = int(c.get("exception_type", "0"))
                except ValueError:
                    continue
                exceptions.setdefault(d, {})[sid] = typ

        by_stop = {}
        stop_time_count = 0
        for stime in rows("stop_times.txt"):
            sid = (stime.get("stop_id") or "").strip()
            tid = (stime.get("trip_id") or "").strip()
            trip = trips.get(tid)
            if sid not in stops or not trip:
                continue
            rid = trip["route_id"]
            stops[sid]["route_ids"].add(rid)
            dep = (stime.get("departure_time") or stime.get("arrival_time") or "").strip()
            arr = (stime.get("arrival_time") or dep).strip()
            if not dep:
                continue
            route = routes.get(rid, {})
            by_stop.setdefault(sid, []).append({
                "stop_id": sid,
                "stop_name": stops[sid]["name"],
                "trip_id": tid,
                "service_id": trip["service_id"],
                "route_id": rid,
                "route_short_name": route.get("short_name") or route.get("long_name") or rid,
                "route_long_name": route.get("long_name", ""),
                "trip_headsign": trip["headsign"],
                "arrival_time": arr,
                "departure_time": dep,
                "stop_sequence": int(stime.get("stop_sequence") or 0),
            })
            stop_time_count += 1

        for sid, stop in stops.items():
            stop["routes"] = [routes[rid] for rid in sorted(stop["route_ids"]) if rid in routes]
            stop["route_ids"] = sorted(stop["route_ids"])

        for sid in by_stop:
            by_stop[sid].sort(key=lambda x: (x["departure_time"], x["route_short_name"]))

        data = {
            "source": "GTFS statique officiel TEC via Belgian Mobility",
            "loaded_at": datetime.now(timezone.utc).isoformat(),
            "counts": {"stops": len(stops), "routes": len(routes), "trips": len(trips), "stop_times": stop_time_count},
            "stops": stops,
            "routes": routes,
            "trips": trips,
            "by_stop": by_stop,
            "calendars": calendars,
            "exceptions": exceptions,
        }
        _static_cache["data"] = data
        _static_cache["loaded_at"] = now
        return data
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Le fichier GTFS statique reçu n'a pas pu être analysé ({type(exc).__name__}).")


def normalize_name(value: str) -> str:
    value = unicodedata.normalize("NFKD", value or "")
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return " ".join("".join(ch.lower() if ch.isalnum() else " " for ch in value).split())


def service_active(data: dict, service_id: str, service_date: date) -> bool:
    exception = data["exceptions"].get(service_date, {}).get(service_id)
    if exception == 1:
        return True
    if exception == 2:
        return False
    cal = data["calendars"].get(service_id)
    return bool(cal and cal["start"] <= service_date <= cal["end"] and service_date.weekday() in cal["weekdays"])


def time_seconds(value: str) -> int:
    parts = value.split(":")
    if len(parts) != 3:
        return 0
    try:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except ValueError:
        return 0


def display_gtfs_time(value: str) -> str:
    # Preserve GTFS times beyond midnight, e.g. 25:10:00 -> 01:10 (+1 j).
    total = time_seconds(value)
    days, remainder = divmod(total, 86400)
    hh, rem = divmod(remainder, 3600)
    mm = rem // 60
    return f"{hh:02d}:{mm:02d}" + (f" (+{days} j)" if days else "")


@app.get("/api/tec/static/summary")
async def static_summary(refresh: bool = False):
    data = await load_static_gtfs(force_refresh=refresh)
    return {"source": data["source"], "loaded_at": data["loaded_at"], "counts": data["counts"]}


@app.get("/api/tec/static/stops")
async def static_stops(q: str = "", limit: int = 5000):
    data = await load_static_gtfs()
    needle = normalize_name(q)
    limit = max(1, min(limit, 20000))
    found = []
    for stop in data["stops"].values():
        if needle and needle not in normalize_name(stop["name"]) and needle not in normalize_name(stop["stop_id"]):
            continue
        found.append({k: v for k, v in stop.items() if k != "route_ids"})
        if len(found) >= limit:
            break
    return {"source": data["source"], "loaded_at": data["loaded_at"], "count": len(found), "stops": found}


@app.get("/api/tec/static/passages")
async def static_passages(q: str = "", stop_id: str = "", date_str: str = "", after: str = "", limit: int = 12):
    """Passages théoriques issus de stop_times.txt, filtrés selon le calendrier GTFS officiel."""
    data = await load_static_gtfs()
    local_now = datetime.now(BRUSSELS)
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else local_now.date()
    except ValueError:
        raise HTTPException(status_code=400, detail="date_str doit être au format AAAA-MM-JJ.")
    after_seconds = None
    if after:
        try:
            hh, mm = after.split(":")[:2]
            after_seconds = int(hh) * 3600 + int(mm) * 60
        except Exception:
            raise HTTPException(status_code=400, detail="after doit être au format HH:MM.")
    limit = max(1, min(limit, 100))

    candidate_ids = []
    if stop_id and stop_id in data["stops"]:
        candidate_ids = [stop_id]
    elif q.strip():
        needle = normalize_name(q)
        exact, partial = [], []
        for sid, stop in data["stops"].items():
            norm = normalize_name(stop["name"])
            if norm == needle:
                exact.append(sid)
            elif needle in norm or norm in needle:
                partial.append(sid)
        candidate_ids = exact if exact else partial[:10]
    else:
        raise HTTPException(status_code=400, detail="Fournis stop_id ou q avec le nom de l'arrêt.")

    if not candidate_ids:
        return {"source": data["source"], "date": target_date.isoformat(), "count": 0, "stops": [], "passages": [], "message": "Aucun arrêt officiel correspondant. Essaie un nom plus court."}

    candidates = []
    passages = []
    for sid in candidate_ids:
        stop = data["stops"][sid]
        candidates.append({"stop_id": sid, "name": stop["name"], "lat": stop["lat"], "lon": stop["lon"], "routes": stop["routes"]})
        for item in data["by_stop"].get(sid, []):
            raw_dep_seconds = time_seconds(item["departure_time"])
            # Un horaire GTFS >= 24:00 appartient au service de la veille
            # et peut correspondre à un passage après minuit le jour demandé.
            if raw_dep_seconds >= 86400:
                service_date = target_date - timedelta(days=1)
                dep_seconds = raw_dep_seconds - 86400
                if after_seconds is not None and after_seconds >= 6 * 3600:
                    continue
            else:
                service_date = target_date
                dep_seconds = raw_dep_seconds
            if not service_active(data, item["service_id"], service_date):
                continue
            if after_seconds is not None and dep_seconds < after_seconds:
                continue
            passages.append({
                "stop_id": sid,
                "stop_name": stop["name"],
                "trip_id": item["trip_id"],
                "service_id": item["service_id"],
                "route_id": item["route_id"],
                "line": item["route_short_name"],
                "destination": item["trip_headsign"] or item["route_long_name"] or "Destination non précisée",
                "arrival_time": display_gtfs_time(item["arrival_time"]),
                "departure_time": display_gtfs_time(str(dep_seconds // 3600).zfill(2) + ":" + str((dep_seconds % 3600) // 60).zfill(2) + ":" + str(dep_seconds % 60).zfill(2)),
                "departure_seconds": dep_seconds,
                "status": "Horaire théorique GTFS",
                "realtime_delay_seconds": None,
            })
    passages.sort(key=lambda x: (x["departure_seconds"], str(x["line"])))
    return {
        "source": data["source"],
        "loaded_at": data["loaded_at"],
        "date": target_date.isoformat(),
        "count": min(len(passages), limit),
        "stops": candidates,
        "passages": passages[:limit],
        "realtime_join": "not_applied",
        "note": "Horaires théoriques officiels. Les retards en temps réel ne sont pas indiqués tant que la correspondance des identifiants GTFS-RT n'est pas confirmée.",
    }
