# CDC Pipeline (Postgres -> Meilisearch)

A minimal CDC pipeline that streams changes from PostgreSQL into Meilisearch and exposes a FastAPI search endpoint.

## File Structure

```
cdc-pipeline/
  .env
  .env.example
  api-frontend/
    Dockerfile
    main.py
    requirements.txt
    templates/
      index.html
  cdc-consumer/
    Dockerfile
    main.py
    requirements.txt
  docker-compose.yml
  init-db/
  init_db/
    init.sql
  lsn_checkpoint.txt
  submission.json
```

## Services

- Postgres: source of truth and logical replication publisher.
- Meilisearch: target search index.
- API: FastAPI app with `/search` endpoint.
- CDC consumer: logical replication client that syncs products to Meilisearch.

## Prerequisites

- Docker Desktop
- Docker Compose (v2+)

## Quick Start

1. Build and start all services:

```
docker-compose up --build
```

2. Insert test data:

```
docker exec -it postgres psql -U postgres -d cdc_db -c "INSERT INTO products (name, description, price) VALUES ('Test Item', 'Example', 19.99);"
```

3. Check Meilisearch stats (PowerShell):

```
curl.exe -H "Authorization: Bearer masterKey" http://localhost:7700/indexes/products/stats
```

4. Search via API:

```
http://localhost:8000/search?q=Test
```

## Important Notes

- Meilisearch requires a primary key named `id`. The CDC consumer maps `product_id` to `id` when syncing.
- Document updates are asynchronous in Meilisearch. Check task status if stats remain at 0.

## Troubleshooting

### Meilisearch shows 0 documents

Check task failures:

```
curl.exe -H "Authorization: Bearer masterKey" http://localhost:7700/tasks?limit=20
```

Common cause:
- `missing_document_id`: ensure documents include `id` (already handled in the consumer).

### PowerShell curl header errors

PowerShell aliases `curl` to `Invoke-WebRequest`. Use:

```
Invoke-WebRequest -Headers @{Authorization="Bearer masterKey"} http://localhost:7700/indexes/products/stats
```

or use real curl:

```
curl.exe -H "Authorization: Bearer masterKey" http://localhost:7700/indexes/products/stats
```

### CDC warnings about wal_level

Postgres is started with logical replication enabled in `docker-compose.yml`. If you changed volumes, run:

```
docker-compose down -v
```

Then start again.

## Environment

Default credentials are baked into compose for local use:

- Postgres user/password: `postgres` / `postgres`
- Meilisearch master key: `masterKey`

Adjust in `.env` if needed.
