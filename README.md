# MarketPulse

MarketPulse is an AI-powered macroeconomic news intelligence system.

It ingests global financial news and converts unstructured text into structured macro insights:
- macroeconomic themes
- key economic events
- entities (countries, institutions, central banks)
- possible market implications

## Product Goal

Build a modular data pipeline that transforms raw news into usable macro signals for analysts and dashboards.

## High-Level Capabilities

- Aggregate news from RSS feeds, financial APIs, and public datasets.
- Normalize and deduplicate article data.
- Apply NLP for embeddings, entity extraction, topic detection, and summarization.
- Detect dynamic macro themes from the article corpus.
- Extract structured macro signals from unstructured text.
- Discover macro narratives by connecting themes and events across articles.
- Expose structured insights through an API for visualization.

## End-to-End Pipeline

```text
News Sources
    ->
Ingestion Pipeline
    ->
Article Processing Pipeline
    ->
Embedding Generation
    ->
Theme Detection
    ->
Structured Macro Insights
    ->
API Layer
    ->
Visualization Dashboard
```

Each stage enriches the data with more structure and meaning.

## Core Pipelines

### 1) News Ingestion Pipeline
Purpose: collect new articles from external sources.

Steps:
- fetch articles from sources
- normalize metadata
- deduplicate content
- store raw article records

Output:
- normalized raw news article records in the database

### 2) Article Processing Pipeline
Purpose: transform raw articles into structured macro insights.

Steps:
- retrieve unprocessed articles
- clean and normalize article text
- generate semantic embeddings
- extract entities
- extract macroeconomic signals
- store processed outputs

Output:
- structured article records with embeddings, extracted entities, macro themes, and metadata

### 3) Theme Detection Pipeline
Purpose: identify emerging macro themes from the article corpus.

Steps:
- retrieve article embeddings
- cluster similar articles
- assign cluster labels
- update theme statistics

Output:
- dynamic macro themes that evolve as new articles arrive

### 4) Narrative Analysis Pipeline
Purpose: identify relationships between macro events and themes.

Steps:
- analyze theme co-occurrence
- connect related economic events
- detect causal chains across articles

Output:
- a graph of macro narratives connecting economic signals

## Component Architecture

### Scripts Layer
Operational entrypoints that trigger jobs.

Responsibilities:
- trigger pipelines
- coordinate batch jobs
- run scheduled processing tasks

Rule: keep scripts thin; delegate logic to pipelines.

### Pipelines Layer
Workflow orchestrators that combine services and repositories.

Responsibilities:
- implement end-to-end business workflows (ingestion, processing, theme detection, narrative analysis)

### Services Layer
Reusable units for focused tasks.

Examples:
- embedding generation
- LLM calls
- entity extraction
- clustering
- source fetching

Rule: services must be reusable across pipelines and API.

### Repositories Layer
Database access layer.

Responsibilities:
- insert/retrieve articles
- store embeddings
- manage themes
- store extracted insights

Rule: isolate all database logic here.

### API Layer
External access to structured macro insights.

Example capabilities:
- retrieve macro themes
- browse processed articles
- semantic search
- narrative insight retrieval

Rule: API reuses services/repositories; no duplicated business logic.

## How Components Interact

Primary data flow:
1. `scripts` trigger pipeline jobs.
2. `pipelines` orchestrate workflow steps.
3. `services` execute reusable operations.
4. `repositories` persist/retrieve data.
5. API endpoints read from services/repositories and expose results.
6. Frontend visualizes API outputs.

Dependency direction:
`scripts -> pipelines -> services -> repositories`

API path:
`api -> services -> repositories`

## Design Principles

- Separation of concerns across layers.
- Reusability of service logic.
- Modular pipelines that are independently testable/extendable.
- Extensibility for future capabilities without breaking core architecture.

## Contributor Orientation

If you add new functionality:
- put focused logic in `services`
- compose flows in `pipelines`
- keep `scripts` as thin entrypoints
- keep DB operations in `repositories`
- expose data via `api` only after service/repository integration

For implementation conventions, see `DEVELOPER_INSTRUCTIONS.md`.

## Frontend

Frontend-specific setup and UI scope live in:
- `frontend/README.md`

Use this root README for product architecture and backend pipeline context.

## Backend Setup (Current)

### 1) Environment variables
Create a `.env` file in **root** folder (in MarketPulse/):

```env
NEWS_API_KEY=...
OPENAI_API_KEY=...
DATABASE_URL=postgresql+psycopg://<user>:<password>@<host>:<port>/postgres?sslmode=require
```

Notes:
- Use Supabase Session Pooler URI if direct DB host is not reachable on your network.
- Keep `sslmode=require`.
# optional JWT secret used by the auth system
SECRET_KEY=your-super-secret-jwt-key
```

### 2) Authentication
The backend currently exposes simple email/password authentication endpoints backed by JWT tokens. Use them for user registration or to protect future API features.

- `POST /auth/register` – register a new user (body: `email`, `password`)
- `POST /auth/login` – obtain a bearer token (body: `email`, `password`)
- `GET /auth/me` – retrieve information about the current user; requires `Authorization: Bearer <token>` header

The frontend stores the access token in `localStorage` and attaches it automatically. Users can sign in, register, and sign out through the UI.

### 2) Install backend dependencies

```bash
cd backend
python -m pip install -r requirements.txt
```

### 3) Initialize database schema
Run `database/schema.sql` once in Supabase SQL Editor.

Or apply from CLI using your configured `DATABASE_URL`:

```bash
cd backend
python scripts/apply_schema.py
```

### 4) Run ingestion pipeline

```bash
python scripts/ingest_news.py --q "inflation" --page-size 20
```

Expected output includes:
- `embedded_count`
- `persisted_count`
- optional `persistence_error` if DB write fails

Theme processing is currently disabled in runtime code while a new theme solution is being rebuilt.

### 5) Verify rows in DB

```sql
select count(*) from public.articles;

select title, canonical_url, created_at
from public.articles
order by created_at desc
limit 10;
```

### 6) Semantic keyword search

Run API server:

```bash
cd backend
uvicorn app.main:app --reload
```

Search with keywords:

```bash
curl "http://127.0.0.1:8000/articles/semantic-search?keywords=inflation%20cooling&limit=5"

```

Or run the CLI:

```bash
cd backend
python scripts/search_articles.py --keywords "inflation cooling" --limit 5
```

### 7) Run frontend UI (semantic search)

```bash
cd frontend
npm run dev
```

Open `http://localhost:3000`.

For proxy configuration and frontend feature details, see `frontend/README.md`.
