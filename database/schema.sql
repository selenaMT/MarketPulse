create extension if not exists vector;
create extension if not exists pgcrypto;

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
