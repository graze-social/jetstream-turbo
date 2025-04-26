# jetstream-turbo

Turbocharged Jetstream messages - hydrates referenced objects, stores to SQLite, punches up to S3 for long term storage.

## Scratch

1. `docker compose build && docker compose up`
2. `docker exec -it jetstream_turbo_service bash`
3. `pdm run turbocharger`

Note: Will almost certainly require round robining session strings. A to-do though everything else is now playing nicely.
