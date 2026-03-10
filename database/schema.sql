create extension if not exists vector;
create extension if not exists pgcrypto;

-- Remove legacy / unrelated theme-management tables.
drop table if exists theme_sync_runs cascade;
drop table if exists theme_maintenance_recommendations cascade;
drop table if exists theme_assignment_logs cascade;
drop table if exists theme_candidate_observations cascade;
drop table if exists theme_candidate_aliases cascade;
drop table if exists theme_lineage cascade;
drop table if exists theme_relations cascade;
drop table if exists theme_snapshots cascade;
drop table if exists theme_aliases cascade;

create table if not exists articles (
  id uuid primary key default gen_random_uuid(),
  source_name text not null,
  source_article_id text null,
  url text not null,
  canonical_url text not null unique,
  title text null,
  description text null,
  content text null,
  author text null,
  language varchar(8) null,
  region text null,
  published_at timestamptz null,
  embedding vector(1536) null,
  embedding_model text null default 'text-embedding-3-small',
  embedded_at timestamptz null,
  metadata jsonb not null default '{}'::jsonb,
  raw_payload jsonb null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_articles_published_at on articles (published_at desc);
create index if not exists idx_articles_source_published_at on articles (source_name, published_at desc);
create index if not exists idx_articles_created_at on articles (created_at desc);
create index if not exists idx_articles_metadata_gin on articles using gin (metadata);
create index if not exists idx_articles_embedding_hnsw
on articles using hnsw (embedding vector_cosine_ops)
where embedding is not null;

create or replace function match_articles(
  query_embedding vector(1536),
  match_count int default 20,
  min_published_at timestamptz default null,
  filter_source text default null
)
returns table (
  article_id uuid,
  canonical_url text,
  title text,
  published_at timestamptz,
  source_name text,
  similarity double precision
)
language sql
stable
as $$
  select
    a.id as article_id,
    a.canonical_url,
    a.title,
    a.published_at,
    a.source_name,
    (1 - (a.embedding <=> query_embedding)) as similarity
  from articles a
  where a.embedding is not null
    and (min_published_at is null or a.published_at >= min_published_at)
    and (filter_source is null or a.source_name = filter_source)
  order by a.embedding <=> query_embedding
  limit greatest(match_count, 1);
$$;

create table if not exists themes (
  id uuid primary key default gen_random_uuid(),
  slug text not null unique,
  canonical_label text not null,
  summary text null,
  status text not null default 'active',
  discovery_method text not null default 'signal',
  first_seen_at timestamptz null,
  last_seen_at timestamptz null,
  title_embedding vector(1536) null,
  article_count integer not null default 0,
  current_snapshot_version integer not null default 0,
  last_snapshot_at timestamptz null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_themes_status
    check (status in ('candidate', 'emerging', 'active', 'cooling', 'dormant', 'retired'))
);

alter table if exists themes
  add column if not exists title_embedding vector(1536),
  add column if not exists article_count integer not null default 0,
  add column if not exists current_snapshot_version integer not null default 0,
  add column if not exists last_snapshot_at timestamptz null;

create unique index if not exists idx_themes_canonical_label_ci
on themes (lower(canonical_label));
create index if not exists idx_themes_status on themes (status);
create index if not exists idx_themes_last_seen_at on themes (last_seen_at desc);
create index if not exists idx_themes_article_count on themes (article_count desc);
create index if not exists idx_themes_title_embedding_hnsw
on themes using hnsw (title_embedding vector_cosine_ops)
where title_embedding is not null;

create table if not exists theme_candidates (
  id uuid primary key default gen_random_uuid(),
  display_label text not null,
  normalized_label text not null unique,
  title_embedding vector(1536) null,
  article_count integer not null default 0,
  status text not null default 'candidate',
  promoted_theme_id uuid null references themes(id) on delete set null,
  first_seen_at timestamptz null,
  last_seen_at timestamptz null,
  promoted_at timestamptz null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint chk_theme_candidates_status
    check (status in ('candidate', 'promoted', 'discarded'))
);

alter table if exists theme_candidates
  add column if not exists title_embedding vector(1536),
  add column if not exists promoted_at timestamptz null;

create index if not exists idx_theme_candidates_status on theme_candidates (status);
create index if not exists idx_theme_candidates_article_count
on theme_candidates (article_count desc, last_seen_at desc);
create index if not exists idx_theme_candidates_title_embedding_hnsw
on theme_candidates using hnsw (title_embedding vector_cosine_ops)
where title_embedding is not null;

create table if not exists theme_article_links (
  id uuid primary key default gen_random_uuid(),
  theme_id uuid not null references themes(id) on delete cascade,
  article_id uuid not null references articles(id) on delete cascade,
  similarity_score double precision not null default 0.0,
  assignment_score double precision not null default 0.0,
  assignment_method text not null default 'semantic',
  assignment_version text not null default 'semantic_v1',
  matched_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (theme_id, article_id)
);

alter table if exists theme_article_links
  add column if not exists similarity_score double precision not null default 0.0,
  add column if not exists assignment_score double precision not null default 0.0,
  add column if not exists assignment_method text not null default 'semantic',
  add column if not exists assignment_version text not null default 'semantic_v1',
  add column if not exists matched_at timestamptz not null default now();

create index if not exists idx_theme_article_links_theme_id on theme_article_links (theme_id);
create index if not exists idx_theme_article_links_article_id on theme_article_links (article_id);
create index if not exists idx_theme_article_links_matched_at on theme_article_links (matched_at desc);

create table if not exists candidate_theme_article_links (
  id uuid primary key default gen_random_uuid(),
  candidate_theme_id uuid not null references theme_candidates(id) on delete cascade,
  article_id uuid not null references articles(id) on delete cascade,
  similarity_score double precision not null default 0.0,
  matched_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (candidate_theme_id, article_id)
);

create index if not exists idx_candidate_theme_article_links_candidate
on candidate_theme_article_links (candidate_theme_id);
create index if not exists idx_candidate_theme_article_links_article
on candidate_theme_article_links (article_id);
create index if not exists idx_candidate_theme_article_links_matched_at
on candidate_theme_article_links (matched_at desc);

create table if not exists historical_themes (
  id uuid primary key default gen_random_uuid(),
  theme_id uuid not null references themes(id) on delete cascade,
  snapshot_version integer not null,
  snapshot_created_at timestamptz not null default now(),
  slug text not null,
  canonical_label text not null,
  summary text null,
  status text not null,
  discovery_method text not null,
  article_count integer not null default 0,
  title_embedding vector(1536) null,
  first_seen_at timestamptz null,
  last_seen_at timestamptz null,
  created_at timestamptz not null default now(),
  unique (theme_id, snapshot_version)
);

create index if not exists idx_historical_themes_theme_time
on historical_themes (theme_id, snapshot_created_at desc);
