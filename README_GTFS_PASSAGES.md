# Backend TEC avec passages GTFS officiels

## Installation
- Conserver `BM_API_KEY` et `BM_API_BASE` dans Render.
- `BM_API_BASE=https://api-management-opendata-production.azure-api.net`
- Déployer `main.py` et `requirements.txt`.

## Routes
- `/api/tec/connection-test` : test des trois flux temps réel.
- `/api/tec/vehicles`, `/api/tec/trips`, `/api/tec/alerts` : flux temps réel existants.
- `/api/tec/static/summary` : statistiques du GTFS statique officiel.
- `/api/tec/static/stops?q=Charleroi&limit=100` : arrêts officiels et lignes desservies.
- `/api/tec/static/passages?q=CHARLEROI%20Gare%20Centrale&limit=12` : prochains horaires théoriques du jour.
- `/api/tec/static/passages?stop_id=IDENTIFIANT_GTFS&date_str=2026-10-17&after=07:00&limit=12` : passages à partir d'une heure donnée.

Le premier appel au GTFS statique télécharge et indexe le ZIP officiel. Il peut prendre un peu de temps. Cache mémoire de six heures par défaut.

Les horaires retournés proviennent de `stop_times.txt` et du calendrier GTFS. Le backend ne simule pas de passages. Il n'affiche pas de retard temps réel tant que la jointure entre les identifiants des courses statiques et GTFS-RT n'est pas confirmée.
