
import os
from datetime import datetime, timezone

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
    version="1.2.0",
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
            detail=(
                "Délai dépassé lors de la connexion "
                "à Belgian Mobility."
            ),
        )

    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail=(
                "Erreur réseau vers Belgian Mobility : "
                f"{type(exc).__name__}."
            ),
        )

    if response.status_code in (401, 403):
        raise HTTPException(
            status_code=502,
            detail=(
                f"Accès refusé par Belgian Mobility "
                f"(HTTP {response.status_code}). "
                "Vérifie l'abonnement, la clé et les autorisations."
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

    # Décodage du flux GTFS-Realtime au format Protocol Buffers.
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
# ROUTE D'ACCUEIL
# ============================================================

@app.get("/")
def home():
    return {
        "service": "TEC Service Contrôle API",
        "version": "1.2.0",
        "status": "online",
        "health": "/health",
        "docs": "/docs",
        "connection_test": "/api/tec/connection-test",
        "static_summary": "/api/tec/static/summary",
    }


# ============================================================
# VERIFICATION DE L'API
# ============================================================

@app.get("/health")
def health():
    # Vérifie la configuration locale, pas les flux distants.
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
# NOUVELLE ROUTE : RESUME DES FLUX TEC
# ============================================================

@app.get("/api/tec/static/summary")
async def static_summary():
    results = {}

    for name, path in FEEDS.items():
        try:
            data = await fetch_feed(path)

            results[name] = {
                "ok": True,
                "count": data["count"],
                "feed_timestamp": data["feed_timestamp"],
                "retrieved_at": data["retrieved_at"],
            }

        except HTTPException as exc:
            results[name] = {
                "ok": False,
                "error": exc.detail,
            }

    return {
        "service": "TEC Service Contrôle API",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "all_ok": all(item["ok"] for item in results.values()),
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
