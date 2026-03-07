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
