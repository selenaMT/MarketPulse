# Developer Instructions (MarketPulse)

This guide is for contributors who are new to web/backend development.
Use it as a "where should this code go?" cheat sheet.

## Core Rule

Keep responsibilities separated:
- `scripts` trigger jobs
- `pipelines` orchestrate workflows
- `services` contain reusable logic
- `repositories` contain database access
- `api` exposes data to clients

Do not skip layers by putting DB queries in scripts or route handlers.

## Quick Mental Model

Think of the backend like a factory line:
1. `scripts` press the "start" button.
2. `pipelines` define the step-by-step workflow.
3. `services` do focused work (embedding, extraction, clustering).
4. `repositories` read/write data.
5. `api` returns data to frontend clients.

If you are unsure, start in `services`, then call it from a `pipeline`.

## Where New Code Should Go (With Examples)

### `backend/scripts/` -> job entrypoints only
Use scripts to start workflows, not to hold business logic.

Example (good):
- `ingest_news.py` calls `IngestionPipeline.run()`
- `process_articles.py` calls `ArticleProcessingPipeline.run()`

Example (bad):
- script directly fetches API data, cleans text, generates embeddings, and writes SQL all in one file

### `backend/app/pipelines/` -> multi-step workflow orchestration
Pipelines coordinate services/repositories for one business flow.

Good responsibilities:
- call multiple services in sequence
- coordinate multi-step processing
- handle pipeline-level retries/status

Example:
- `ArticleProcessingPipeline.run()`:
1. load unprocessed articles via repository
2. clean text via text service
3. generate embeddings via embedding service
4. extract entities via entity service
5. save outputs via repository

Avoid:
- writing direct SQL in pipeline files
- building HTTP request/response objects

### `backend/app/services/` -> focused reusable logic
Services should be small and reusable in pipelines or API.

Good examples:
- embedding service
- entity extraction service
- clustering service
- source-fetching service

Concrete examples:
- `EmbeddingService.generate(text: str) -> list[float]`
- `EntityExtractionService.extract(text: str) -> list[Entity]`
- `ThemeClusteringService.cluster(vectors) -> list[ThemeCluster]`

Avoid examples:
- pipeline scheduling concerns
- endpoint-specific response formatting

### `backend/app/repositories/` -> database access only
Repositories are the only place for DB queries and persistence logic.

Good examples:
- insert/select/update article records
- load embeddings for clustering
- store extracted insights/themes

Concrete examples:
- `ArticleRepository.get_unprocessed(limit=100)`
- `ArticleRepository.save_processed_article(article_id, embedding, entities, signals)`
- `ThemeRepository.upsert_theme_stats(theme_id, count, last_seen_at)`

Avoid examples:
- NLP/business workflow logic
- calling external AI APIs

### `backend/app/api/` -> endpoint and I/O layer
API files handle HTTP concerns and call service/repository-backed logic.

Good examples:
- call services/repositories to serve data
- validate inputs and shape outputs

Example:
- `GET /themes`:
1. read query params
2. call a service/repository method
3. return JSON response

Avoid examples:
- duplicating pipeline logic
- writing raw SQL inside handlers

## Example: One Feature, Proper Layering

Feature request: "Add sentiment signal per article."

Put code here:
1. `services/sentiment_service.py` -> model/API call that returns sentiment.
2. `pipelines/article_processing_pipeline.py` -> call sentiment service in workflow.
3. `repositories/article_repository.py` -> save/load sentiment field.
4. `api/articles.py` -> expose sentiment in response.
5. optional `scripts/reprocess_sentiment.py` -> batch re-run old articles.

Do not put sentiment logic directly in script or API route.

## Typical Change Workflow

When adding a new capability:
1. Implement the focused logic in a `service`.
2. Integrate it into the right `pipeline`.
3. Add/update repository methods if new data must be stored.
4. Expose results in API if needed.
5. Add or update a thin script entrypoint only if operational execution is needed.

## Naming Suggestions (for consistency)

- Pipelines: `*_pipeline.py`
- Services: `*_service.py`
- Repositories: `*_repository.py`
- Scripts: verb-oriented, e.g. `ingest_news.py`, `process_articles.py`

## Code Organization Checklist

Before opening a PR, verify:
- business workflow is in `pipelines`
- reusable logic is in `services`
- DB logic is only in `repositories`
- scripts are thin entrypoints
- API layer reuses services/repositories

## Practical Notes for This Repo

- Keep frontend and backend concerns separate.
- Frontend should consume API outputs, not re-implement backend processing.
- Prefer small, focused files over large multipurpose modules.
- If a file grows too much, split by responsibility instead of adding mixed logic.

## Database Conventions (Current)

- Database: Supabase PostgreSQL accessed via SQLAlchemy + psycopg.
- Connection config: root `.env` (`DATABASE_URL`), loaded by scripts and DB session module.
- Schema bootstrap: `database/schema.sql` is the current source of truth for initial setup.
- Migrations: not enabled yet in this phase.

### Current persistence flow

1. `backend/scripts/ingest_news.py` starts ingestion job.
2. `backend/app/pipelines/news_ingestion_pipeline.py` fetches + embeds.
3. `backend/app/repositories/article_repository.py` upserts into `articles`.

### Rules

- Keep SQL/DB writes in repositories only.
- Pipelines orchestrate; they do not contain SQL.
- If persistence fails, surface clear error messages to script output.

### Supabase connectivity note

- If direct host (`db.<project-ref>.supabase.co`) fails on IPv4 networks, use Supabase Session Pooler URI.
- Keep `?sslmode=require` in `DATABASE_URL`.
