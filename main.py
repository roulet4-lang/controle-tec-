
import os
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict

app = FastAPI(title="TEC Service Contrôle API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://roulet4-lang.github.io"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

API_KEY = os.getenv("BM_API_KEY", "")
API_BASE = os.getenv("BM_API_BASE", "").rstrip("/")

FEEDS = {
    "vehicles": "/api/gtfs/feed/tec/rt/vehicle-position",
    "trips": "/api/gtfs/feed/tec/rt/trip-update",
    "alerts": "/api/gtfs/feed/tec/rt/alert",
}


async def fetch_feed(path):
    if not API_KEY or not API_BASE:
        raise HTTPException(
            status_code=503,
            detail="BM_API_KEY ou BM_API_BASE non configurée sur Render.",
        )

    try:
        async with httpx.AsyncClient(timeout=25) as client:
            response = await client.get(
                API_BASE + path,
                headers={
                    "Ocp-Apim-Subscription-Key": API_KEY,
                    "Accept": "application/x-protobuf",
                },
            )
    except httpx.HTTPError:
        raise HTTPException(
            status_code=502,
            detail="Connexion à Belgian Mobility impossible.",
        )

    if response.status_code in (401, 403):
        raise HTTPException(
            status_code=502,
            detail="Accès refusé par Belgian Mobility. Vérifie l'abonnement.",
        )

    if response.status_code == 429:
        raise HTTPException(
            status_code=503,
            detail="Limite de requêtes API atteinte.",
        )

    if response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail=f"Belgian Mobility a répondu HTTP {response.status_code}.",
        )

    feed = gtfs_realtime_pb2.FeedMessage()

    try:
        feed.ParseFromString(response.content)
    except Exception:
        raise HTTPException(
            status_code=502,
            detail="Réponse reçue, mais format GTFS-RT invalide.",
        )

    data = MessageToDict(
        feed,
        preserving_proto_field_name=True,
        use_integers_for_enums=True,
    )

    return {
        "source": "Belgian Mobility / TEC",
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "feed_timestamp": data.get("header", {}).get("timestamp"),
        "count": len(data.get("entity", [])),
        "entities": data.get("entity", []),
    }


@app.get("/")
def home():
    return {
        "service": "TEC Service Contrôle API",
        "health": "/health",
        "docs": "/docs",
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "api_key_configured": bool(API_KEY),
        "api_base_configured": bool(API_BASE),
    }


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
        "all_ok": all(x["ok"] for x in results.values()),
        "feeds": results,
    }


@app.get("/api/tec/vehicles")
async def vehicles():
    return await fetch_feed(FEEDS["vehicles"])


@app.get("/api/tec/trips")
async def trips():
    return await fetch_feed(FEEDS["trips"])


@app.get("/api/tec/alerts")
async def alerts():
    return await fetch_feed(FEEDS["alerts"])
